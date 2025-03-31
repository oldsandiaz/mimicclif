import logging
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Dict, Any, List, Callable
from src.utils import save_to_rclif
import functools
import inspect

def intm_store_in_dev(method):
    '''
    Decorator to add extra storage of intermediate results for debugging during development
    (but not so for the actual prod run).
    '''
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        # Call the original method
        df_out = method(self, *args, **kwargs)
        # Store the result if in dev_mode
        if self.dev_mode:
            self.data[method.__name__] = df_out
        return df_out
    return wrapper

class MimicToClifBasePipeline(ABC):
    """Base class for all ETL pipelines in the project."""
    
    def __init__(self, clif_table_name: str, dev_mode: bool = False):
        self.clif_table_name = clif_table_name
        self.logger = logging.getLogger(f"etl.{clif_table_name}")
        self.data: Optional[Dict[str, Any]] = None
        self._dev_mode = dev_mode
        self._step_results: Dict[str, Any] = {}
        self._current_step: Optional[str] = None
        self._step_order: List[str] = []
        self._method_dependencies: Dict[str, List[str]] = {}
        
    def _track_step(self, func: Callable) -> Callable:
        """Decorator to track step execution and results."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            step_name = func.__name__
            self.logger.info(f"Running step: {step_name}")
            
            try:
                result = func(*args, **kwargs)
                self._step_results[step_name] = result
                self._current_step = step_name
                if step_name not in self._step_order:
                    self._step_order.append(step_name)
                return result
            except Exception as e:
                self.logger.error(f"Error in step {step_name}: {e}")
                raise e
        return wrapper
    
    def _get_method_dependencies(self, method_name: str) -> List[str]:
        """Get the list of methods that need to be run before the given method."""
        if method_name not in self._method_dependencies:
            # Get the method's source code
            method = getattr(self, method_name)
            source = inspect.getsource(method)
            
            # Find all method calls in the source code
            dependencies = []
            for line in source.split('\n'):
                if 'self.' in line:
                    # Extract method name from self.method_name() calls
                    method_call = line.split('self.')[1].split('(')[0]
                    if method_call != method_name and method_call not in dependencies:
                        dependencies.append(method_call)
            
            self._method_dependencies[method_name] = dependencies
            
        return self._method_dependencies[method_name]
    
    def run_up_to(self, method_name: str) -> Any:
        """Run the pipeline up to and including the specified method.
        
        Args:
            method_name: Name of the method to run up to
            
        Returns:
            The result of the specified method
        """
        if not hasattr(self, method_name):
            raise ValueError(f"Method {method_name} not found in pipeline")
            
        # Get all dependencies for this method
        dependencies = self._get_method_dependencies(method_name)
        
        # Run all dependencies first
        for dep in dependencies:
            if dep not in self._step_results:
                self.run_up_to(dep)
        
        # Run the requested method
        method = getattr(self, method_name)
        return method()
    
    def get_step_result(self, method_name: str) -> Any:
        """Retrieve the result of a specific pipeline step."""
        return self._step_results.get(method_name)
    
    def get_current_step(self) -> Optional[str]:
        """Get the current step of the pipeline execution."""
        return self._current_step
    
    def get_available_steps(self) -> List[str]:
        """Get a list of all available steps in the pipeline."""
        return self._step_order
    
    def get_last_result(self) -> Any:
        """Get the result of the most recently executed step."""
        if not self._step_order:
            return None
        return self._step_results.get(self._step_order[-1])
    
    def clear_results(self) -> None:
        """Clear all stored step results."""
        self._step_results.clear()
        self._step_order.clear()
        self._current_step = None
        self.data = None
    
    def run(self) -> None:
        """Run the complete pipeline."""
        self.logger.info(f"Starting {self.clif_table_name} pipeline")
        try:
            self.extract()
            self.transform()
            self.load()
            self.logger.info(f"Completed {self.clif_table_name} pipeline successfully")
        except Exception as e:
            self.logger.error(f"Error in {self.clif_table_name} pipeline: {e}")
            raise e

    @abstractmethod
    def extract(self) -> Dict[str, Any]:
        """Extract data from source"""
        pass
        
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Transform the extracted data"""
        pass
        
    def load(self) -> None:
        """Load transformed data to destination"""
        df_out = self.data[self.transform.__name__]
        save_to_rclif(df_out, self.clif_table_name)
        self.logger.info(f"Saved {self.clif_table_name} to parquet file")
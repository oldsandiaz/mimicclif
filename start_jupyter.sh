#!/bin/bash

# Set the default partition with a prefix of "tier"
PART_SUFFIX=${1:-2q}  # Default to "2q" if no argument is provided
PARTITION="tier${PART_SUFFIX}"

# Request resources and start an interactive session with the specified or default partition
srun --pty -p "$PARTITION" --mem 150G --time 30:00:00 /bin/bash << EOF

# Load necessary modules
module load gcc/12.1.0 python/3.10.5

# Start Jupyter Notebook
jupyter-notebook --no-browser --ip=0.0.0.0

EOF

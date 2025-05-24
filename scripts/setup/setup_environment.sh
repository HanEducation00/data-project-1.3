
#!/bin/bash

# Data Platform 1.2 - Environment Setup
set -e

echo "🚀 Setting up Data Platform 1.2..."

# Check conda
if ! command -v conda &> /dev/null; then
    echo "❌ Conda not found. Install Anaconda/Miniconda first."
    exit 1
fi

# Remove existing environment if exists
if conda env list | grep -q "data-platform-1.2"; then
    echo "🗑️ Removing existing environment..."
    conda env remove -n data-platform-1.2 -y
fi

# Create environment
echo "📦 Creating environment from environment.yml..."
conda env create -f environment.yml

# Activate environment
echo "🔄 Activating environment..."
source $(conda info --base)/etc/profile.d/conda.sh
conda activate data-platform-1.2

# Verify installation
echo "🔍 Verifying installation..."
echo "Python: $(python --version)"
echo "PySpark: $(python -c 'import pyspark; print(pyspark.__version__)')"

echo "✅ Setup complete! Activate with: conda activate data-platform-1.2"
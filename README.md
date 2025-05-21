# Delta Arctic Demo

This repository contains a demo project for Delta Arctic using Atlan.

The project uses `uv` for environment management. If `uv` is installed, you can run `uv run main.py` to automatically create a Python virtual environment, install all requirements, and start the app.

## Project Overview

- **Purpose:** Demonstrate Delta Arctic features and integration with Atlan.

## Getting Started

1. **Clone the repository:**
    ```bash
    git clone https://github.com/your-org/DeltaArcticDemo.git
    cd DeltaArcticDemo
    ```

2. **Configure environment variables:**
    - Update the `.env` file with your `ATLAN_API_KEY`.
    - If running `example_pii_classification.ipynb`, also set the `OPENAPI_API_KEY`.

3. **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Run the demo:**
    ```bash
    python main.py
    ```

## Building the AWS Lambda Package

To build the AWS Lambda deployment package, run:

```bash
./build_lambda.sh
```

This script packages the Lambda function and its dependencies into a `.zip` file for AWS Lambda deployment.

**Note:** If you update Python dependencies using `uv add`, run `uv pip freeze > requirements.txt` before building the package.

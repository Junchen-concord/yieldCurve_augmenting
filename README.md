# yieldCurve_augmenting
DATA-897

## Project idea (brainstorming)
- Smooth out drops in certain pay-in lines by accounting for returns and recurring payments from earlier periods.
- Overlay default rate metrics (FPD–AA) on top of installment-based yield curves to help explain variations in pay-in rates.

## Local setup
1. Create virtual environment
   - macOS/Linux:
     - `python3 -m venv .venv`
     - `source .venv/bin/activate`
2. Install dependencies
   - `pip install -r requirements.txt`
3. Register Jupyter kernel (optional)
   - `python -m ipykernel install --user --name yieldCurve_augmenting --display-name "Python (.venv) - yieldCurve_augmenting"`

# County Scraper
## Editing Code
- Make sure you have Jupyter Installed
- Open the Jupyter Notebook `Jupyter notebook`  or  `Jupyter Lab`
- Navigate to the Jupyter notebook you want to edit
## Exporting the Jupyter Notebook to a python script
### In Jupyter notebook
- `File \>\> export notebook to >> excutable script`
### From CMD line
- `https://stackoverflow.com/questions/17077494/how-do-i-convert-a-ipython-notebook-into-a-python-file-via-commandline`

- `jupyter nbconvert --to script [YOUR_NOTEBOOK].ipynb`
## Deployment
- Navigate to Serverless
- `sls deploy`


## Changing schedule
- go to `http://www.cronmaker.com/`
- write the frequency you want to have
- add it to serverless.yml under events \>\> schedule 
import io
import time
import re
from bs4 import BeautifulSoup
import pandas as pd
from cache import *

url_template = "https://data.gov.uk/search?filters%5Bformat%5D=CSV&filters%5Bpublisher%5D=Office+for+National+Statistics&filters%5Btopic%5D=&page={page}&q=lookup&sort=best"

csv_index = 1

md = "# ONS Lookups\n\n"

name_rows = []
lookup_rows = []

def get_batch(batch_num):
    global md
    html = get_page(url_template.format(page=batch_num))
    soup = BeautifulSoup(html, "html.parser")
    links = soup.select('a[href*="/dataset/"]', class_="govuk-link")
    for link in links:
        href = 'https://data.gov.uk' + link['href']
        text = link.text.replace("_", "\\_")
        md += "## " + text + "\n\n"
        get_dataset(href)

def get_dataset(url):
    global csv_index, md
    html = get_page(url)
    soup = BeautifulSoup(html, "html.parser")
    summary = soup.find('div', class_="js-summary")
    summary_p = summary.find_all('p')
    for item in summary_p:
        md += item.text.replace("_", "\\_") + "\n\n"
    links = soup.select('a[data-ga-format="CSV"]', class_="govuk-link")
    for link in links:
        href = link['href']
        if href.endswith('.csv'):
            csv = get_page(href)
            if csv.startswith("ï»¿"):
                csv = csv[3:]
                process_csv(csv)
                csv_filename = f"csv/{csv_index:03}.csv"
                md += "[CSV]({})\n\n".format(csv_filename)
                with open(csv_filename, "w") as f:
                    f.write(csv)
                csv_index += 1

def process_csv(csv):
    df = pd.read_csv(io.StringIO(csv), dtype='string')
    df.columns = [x.upper() for x in df.columns]
    columns = [c for c in df.columns]
    code_columns = [c for c in columns if c.endswith('CD')]
    name_columns = {c: c[:-2] + 'NM' for c in code_columns if c[:-2] + 'NM' in columns}
    for row in df.itertuples():
        for i, col1 in enumerate(code_columns):
            if col1 in name_columns:
                name = getattr(row, name_columns[col1])
                name_rows.append([col1, getattr(row, col1), name])
            for col2 in code_columns[i+1:]:
                code1 = getattr(row, col1)
                code2 = getattr(row, col2)
                lookup_rows.append([col1, code1, col2, code2])

if __name__ == "__main__":
    for batch in range(1, 16):
        print(f"batch {batch}")
        get_batch(batch)
    lookup = pd.DataFrame(lookup_rows, columns=["type1", "code1", "type2", "code2"])
    names = pd.DataFrame(name_rows, columns=["type", "code", "name"])
    lookup = lookup.drop_duplicates()
    names = names.drop_duplicates()
    print(lookup)
    print(names)
    lookup.to_csv("generated_csv/lookup.csv", index=False)
    names.to_csv("generated_csv/names.csv", index=False)
    with open("datasets.md", "w") as f:
        f.write(md)

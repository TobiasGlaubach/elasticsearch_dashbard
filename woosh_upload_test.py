
import os
from icecream import ic
from langchain_community.document_loaders import PyPDFLoader
from langchain_community.document_loaders import UnstructuredPDFLoader
from langchain_community.document_loaders import PDFMinerLoader
from langchain_community.document_loaders import PyMuPDFLoader

from langchain_community.document_loaders import DirectoryLoader

import glob

# loader = DirectoryLoader('../', glob="C:/Users/tobia/Nextcloud/Shared/MKE*.md", use_multithreading=False, show_progress=True)
# docs = loader.load()

# ic(docs)
# def load_pdf(pth):
#     loader = PyPDFLoader(pth)
#     pages = loader.load_and_split()
#     return pages
# def load_pdf(pth):
#     loader = UnstructuredPDFLoader(pth)
#     pages = loader.load_and_split()
#     return pages

# def load_pdf(pth):
#     loader = PDFMinerLoader(pth)
#     pages = loader.load_and_split()
#     return pages

# ic(PyMuPDFLoader('https://cloud.mpifr-bonn.mpg.de/index.php/f/19442801').load_and_split())


load_pdf = lambda pth: PyMuPDFLoader(pth).load_and_split()

loaders = {'pdf': load_pdf}

from pathlib import Path
home = str(Path.home())

filtfun = lambda f: f.split('.')[-1] in loaders and f.startswith('MKE')

path = "C:/Users/tobia/Nextcloud/Shared/"
fn = []
for dirpath, dirnames, filenames in os.walk(path):
    fn += [os.path.join(dirpath, filename).replace("\\", "/") for filename in filenames if filtfun(filename)]
ic(len(fn))
ic(fn)

for path in fn:
    doc = load_pdf(path)
    for i, page in enumerate(doc):
        print(i, len(doc), '-'*100)
        ic(page)
    break

from whoosh.index import create_in
from whoosh.fields import Schema, STORED, ID, KEYWORD, TEXT, NUMERIC

schema = Schema(title=TEXT(stored=True), content=TEXT(stored=True), source=ID(stored=True), page=NUMERIC(stored=True), ltc=TEXT(stored=True), link=TEXT(stored=True))

ix = create_in('C:/temp/Wooosh/', schema)
writer = ix.writer()


for path in fn:
    doc = load_pdf(path)
    ic(path)
    title = os.path.basename(path)
    todoc = lambda page: writer.add_document(title=title, content=page.page_content, source=page.metadata['source'], page=page.metadata['page'])
    for page in doc:
        todoc(page)

writer.commit()


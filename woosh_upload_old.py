
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

path = "C:/Users/tobia/Nextcloud/Shared/"
fn = []
for dirpath, dirnames, filenames in os.walk(path):
    fn += [os.path.join(dirpath, filename).replace("\\", "/") for filename in filenames if filename.split('.')[-1] in loaders]
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

schema = Schema(title=TEXT(stored=True), content=TEXT, source=ID(stored=True), page=NUMERIC, n_pages=NUMERIC, ltc=TEXT, link=TEXT)

ix = create_in('C:/temp/Wooosh2/', schema)
writer = ix.writer()


for path in fn:
    doc = load_pdf(path)
    ic(path)
    title = os.path.basename(path)
    todoc = lambda page: writer.add_document(title=title, content=page.page_content, source=page.metadata['source'], page=page.metadata['page'], n_pages=page.metadata['total_pages'])
    for page in doc:
        todoc(page)

writer.commit()

from whoosh.qparser import QueryParser
with ix.searcher() as searcher:
    query = QueryParser("content", ix.schema).parse("contractor")
    results = searcher.search(query)
    for result in results:
        ic(result)


# def crawl(path, n_max=-1):

#     files = {}
#     fn = []
#     for dirpath, dirnames, filenames in os.walk(path):
#         fn += [os.path.join(dirpath, filename).replace("\\", "/") for filename in filenames if filename.split('.')[-1] in loaders]
#     ic(len(fn))

#     for s in fn:
#         ext = s.split('.')[-1]

#         if '.git' in s or '__pycache__' in s or '.ipynb_checkpoints' in s or s.endswith('.ipynb'):
#             continue

#         if not os.path.exists(s):
#             ic('MISSING FILE --> skipping')
#             ic(s)
#             continue

#         ic('FOUND FILE: ' + s)
#         files[s] = loaders[ext]

#         if n_max > 0 and len(files) > n_max:
#             ic(f'stopping looking for new files because batch size of {n_max} has been reached.')
#             break
        
#     return files

# files = crawl(r"C:\Users\tobia\Nextcloud\Shared")


# for path, loader in files.items():
#     if os.path.basename(path).startswith('MKE'):
#         doc = loader(path)
#         for i, page in enumerate(doc):
#             print(i, len(doc), '-'*100)
#             ic(page)
#         break


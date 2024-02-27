import os
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, STORED, ID, KEYWORD, TEXT, NUMERIC
from whoosh import highlight, writing
from whoosh.qparser import QueryParser

from dash import dash_table, dcc, html


schema = Schema(title=TEXT(stored=True), content=TEXT(stored=True), source=ID(stored=True), page=NUMERIC(stored=True), n_pages=NUMERIC(stored=True), ltc=TEXT(stored=True), link=TEXT(stored=True))
fields = 'title source page n_pages ltc link checksum'.split()













ix = open_dir('C:/temp/Wooosh/')

reader = ix.reader()

def get_doc(id_):
    with ix.searcher() as searcher:
        return searcher.stored_fields(id_)

def hit2dc(hit, **kwargs):
    dc = {k:hit[k] for k in fields}
    dc['content'] = hit.content
    dc['highlights'] = hit.highlights("content")
    dc['score'] = hit.score
    dc = {**dc, **kwargs}
    return dc

class HtmlMarkFormatter(highlight.Formatter):
    """Puts html.mark around the matched terms.
    """
    def format_token(self, text, token, replace=False):
        tokentext = highlight.get_text(text, token, replace)
        return "<mark>%s</mark>" % tokentext
    

def wsearch(searchstring, field='content', formatter=None, fragmenter=None):

    if fragmenter is None:
        fragmenter = highlight.SentenceFragmenter()
    if formatter is None:
        formatter = highlight.UppercaseFormatter()

    with ix.searcher() as searcher:
        query = QueryParser(field, ix.schema).parse(searchstring)
        
        results = searcher.search(query, terms=True)
        results.fragmenter = fragmenter
        results.formatter = formatter
        
        res = []
        for hit in results:
            dc = searcher.stored_fields(hit.docnum)
            dc['highlights'] = hit.highlights("content")
            dc['score'] = hit.score
            res.append(dc)

        return res, results.runtime
    
def format_res(res):
    pass

def doc2item(hit, tempstore):
    
    comps = [html.H6(f"filename: {hit['title']}"),
            html.Div('ITEM ID: {} | Match Score: {}'.format(hit['source'], hit['score'])),
            html.Div(f"page: {hit['page']}/{hit['n_pages']}")]

    if 'link' in hit and hit['link']:
        lnk = hit['link'] #+ '#page={}'.format(doc['page'])
        comps.append(html.Div(html.A(lnk, href=lnk, target="_blank",  rel="noopener noreferrer")))
        # comps.append(html.Link(title=doc['_source'], href=lnk, target='_blank',  rel="noopener noreferrer"))

    if hit['content'] not in tempstore:
        tempstore.add(hit['content'])
        el = html.Pre(html.Code(hit['highlights']), style={"border":"3px", "border-style":"solid", "padding": "1em"})

        comps += [html.Br(), 
            html.Div(el, style={
                "width": "80%",
                "height": "300px",
                "overflow": "auto"}), 
            html.Hr()]
    else:
        comps += [html.Div(f"content already shown in search results")]
    return html.Div(comps)


def dsearch(searchtext, **kwargs):
    tempstore = set()
    return [doc2item(hit, tempstore) for hit in wsearch(searchtext, **kwargs)]















from langchain_community.document_loaders import PyMuPDFLoader




load_pdf = lambda pth: PyMuPDFLoader(pth).load_and_split()
filtfun = lambda f: f.split('.')[-1] == 'pdf' and f.startswith('MKE')

def clean_index():
    ix = create_in('C:/temp/Wooosh/', schema)
    writer = ix.writer()
    writer.commit(mergetype=writing.CLEAR)


def crawl_dir(path, progress_bar=True):
    if progress_bar:
        from tqdm import tqdm
    else:
        tqdm = lambda args: args

    fn = []
    for dirpath, dirnames, filenames in os.walk(path):
        fn += [os.path.join(dirpath, filename).replace("\\", "/") for filename in filenames if filtfun(filename)]
        

    ix = create_in('C:/temp/Wooosh/', schema)
    writer = ix.writer()


    for path in tqdm(fn):
        doc = load_pdf(path)
        title = os.path.basename(path)
        for i, page in enumerate(doc, 1):
            writer.add_document(title=title, 
                                content=page.page_content, 
                                source=page.metadata['source'], 
                                page=page.metadata['page'] +1 if 'page' in page.metadata else i, 
                                n_pages=page.metadata['total_pages'])

    writer.commit()



if __name__ == "__main__":
    clean_index()
    crawl_dir("C:/Users/tobia/Nextcloud/Shared/")
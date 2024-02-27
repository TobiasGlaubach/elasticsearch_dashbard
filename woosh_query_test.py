from icecream import ic
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, STORED, ID, KEYWORD, TEXT, NUMERIC
from whoosh import highlight

ix = open_dir('C:/temp/Wooosh/')

class HtmlMarkFormatter(highlight.Formatter):
    """Puts html.mark around the matched terms.
    """
    def format_token(self, text, token, replace=False):
        tokentext = highlight.get_text(text, token, replace)
        return "<mark>%s</mark>" % tokentext
    

from whoosh.qparser import QueryParser
with ix.searcher() as searcher:
    query = QueryParser("content", ix.schema).parse("mpifr")
    results = searcher.search(query)
    ic(results.runtime)
    for result in results:
        ic(result.score)
    

    results = searcher.search(query, terms=True)
    results.fragmenter = highlight.SentenceFragmenter()
    results.formatter = HtmlMarkFormatter()

    res = []
    for hit in results:
        res.append('<hr>')
        res.append('<p>{}</p>'.format(hit["title"]))
        res.append('<p>{}</p>'.format(hit.score))
        res.append('<code style=border:1px solid black;>{}</code>'.format(hit.highlights("content")))
        
    with open('result.html', 'wb') as fp:
        fp.write('\n'.join(res).encode('utf8'))
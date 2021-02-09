# importing required modules 
import io 

from pdfminer.converter import XMLConverter, HTMLConverter, TextConverter
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage

import bs4 as bs

#%%

def extract_pdf2str(fp, outtype="html", verbose=0, maxpages = 0, password = b'', pagenos = set(), caching = True, rotation = 0, imagewriter = None, laparams=None):
    
    outtype = outtype.lower()
    outfp = None
    debug = 0
    stripcontrol = False
    layoutmode = 'normal'
    scale = 1

    # elif outtype == 'tag':
    #     device = TagExtractor(rsrcmgr, outfp)

    
    if laparams is None: laparams = LAParams()

    
    # device = TextConverter(rsrcmgr, outfp, laparams=laparams,
    #                             imagewriter=imagewriter)

    pages = []
    if verbose > 0: print("PROCESSING: Page ", end='')
    with io.StringIO() as outfp:
        rsrcmgr = PDFResourceManager(caching=caching)
        if outtype == 'text' or outtype == "txt":
            device = TextConverter(rsrcmgr, outfp, laparams=laparams,
                                imagewriter=imagewriter)
        elif outtype == 'xml':
            device = XMLConverter(rsrcmgr, outfp, laparams=laparams,
                                imagewriter=imagewriter,
                                stripcontrol=stripcontrol)
        elif outtype == 'html':
            device = HTMLConverter(rsrcmgr, outfp, scale=scale,
                                layoutmode=layoutmode, laparams=laparams,
                                imagewriter=imagewriter, debug=debug)
        else:
            raise ValueError('outtype must be one of the following "text", "html", "xml"')


        interpreter = PDFPageInterpreter(rsrcmgr, device)
    
        for i_page, page in enumerate(PDFPage.get_pages(fp, pagenos,
                                    maxpages=maxpages, password=password,
                                    caching=caching, check_extractable=True)
        ):
            
            if verbose > 0: print(f'{i_page} ', end="")
            try:
                page.rotate = (page.rotate+rotation) % 360
                interpreter.device.outfp = outfp
                interpreter.process_page(page)
                
    
                pages.append(outfp.getvalue())
    
                outfp.truncate(0)
                outfp.seek(0)
                
            except Exception as e:
                print('ERROR while processing page (msg: {}.. skipping for now'.format(e))
                
        device.close()
        pages.append(outfp.getvalue())
            
        
        print('DONE!')
        
        return pages


def convert_html2text(pages_in, is_converted_pdf=True):

    pages = []
    for article in pages_in:
        # parsed_article = bs.BeautifulSoup(article,'lxml')
        parsed_article = bs.BeautifulSoup(article,"html.parser") 
        divs = parsed_article.find_all('div')
        article_text = "".join([d.text for d in divs if (d is not None) and d.text])
        pages.append(article_text)

    if is_converted_pdf:
        txt = pages[-1]
        page_nos_str = txt[len('Page: '):].split(',')
        page_nos_int = [int(no) for no in page_nos_str]
        if len(pages) > len(page_nos_int):
            pages = pages[:len(page_nos_int)] 
            
    else:
        page_nos_int = list(range(len(pages)))
                
    return page_nos_int, pages



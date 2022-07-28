#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().system('python -m pip install lxml psycopg2')


# In[ ]:


from lxml import etree


# In[36]:


doc = etree.parse('pubmed22n0003.xml')

root = doc.getroot()

print(len(root.find('.')))


# In[50]:


# Database connection
import psycopg2
from psycopg2 import OperationalError


# In[51]:


def create_connection(db_name, db_user, db_password, db_host,db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database = db_name,
            user = db_user,
            password = db_password,
            host = db_host,
            port = db_port
        )
        print('Connection to PostgreSQL is successful')
    except OperationalError as oe:
        print(f'The error {e} occurred while making the connection')
    
    return connection
        


# In[52]:


connection = create_connection('cbonam', 'cbonam', '001038223', '10.80.28.228', '5432') 


# In[ ]:


# Inserting data into author (forename,lastname)
from tqdm import tqdm
for i in tqdm(range(len(root.find('.')))):
    pubmed_article = root.find('.')[i]
    medline_citation = pubmed_article.findall("./MedlineCitation/")
    pmid = medline_citation[0].text
    
    authors_list = []
    if medline_citation[1].tag != 'DateCompleted':
        date_completed = '01-01-2001'
    else:
        date = medline_citation[1].findall('./')
        year, month, day = date[0].text, date[1].text, date[2].text
        date_completed = year+month+day
        #print(str(len(medline_citation)+"length of the medlinecitation")
    if len(medline_citation) < 7:
        print("inside iff")
        print(medline_citation)
        if medline_citation[2].tag == 'Article':
            article = medline_citation[2].findall('./')
            for y in article:
                if y.tag=="AuthorList":
                    author_list = y.getchildren()
            #author_list = article[5].findall('./')
            # print(i, len(author_list), author_list)
            for j in range(len(author_list)):
                author_info = []
                author_id = pmid + '_' + str(j)
                author = author_list[j].findall('./')
                last_name = author[0].text
                fore_name = author[1].text
                author_info.append(author_id)
                author_info.append(last_name)
                author_info.append(fore_name)
                authors_list.append(tuple(author_info))
            # journal_issn = journal[0].text
            title = article[1].text
            
        else:
            article = medline_citation[3].findall('./')
            for y in article:
                if y.tag=="AuthorList":
                    author_list = y.getchildren()
            #author_list = article[3].findall('./')
            print(author_list)
            for j in range(len(author_list)):
                author_info = []
                last_name=''
                fore_name=''
                author_id = pmid + '_' + str(j)
                author = author_list[j].findall('./')
                print()
                print(len(author))
                if(len(author)==1):
                    last_name = author[0].text
                else:
                    last_name = author[0].text
                    fore_name = author[1].text
                author_info.append(author_id)
                author_info.append(last_name)
                author_info.append(fore_name)
                authors_list.append(tuple(author_info))
            # journal_issn = journal[0].text
            title = article[1].text
    else:
        article = medline_citation[3].findall('./')
        print("inside the else ")
        
        if article[3].tag=='Abstract':
            author_list=article[4].findall('./')
            print("got it hurry")
            
            print(author_list)
            for j in range(len(author_list)):
                last_name=''
                fore_name=''
                author_id = pmid + '_' + str(j)
                author = author_list[j].findall('./')
                print(author)
                author_info = []
                print(str(len(author))+"length of author")
                print(author[0].text)
                if author[0].tag != 'LastName':
                    continue
                else:
                    if(len(author)==1):
                        last_name = author[0].text
                    else:
                        last_name = author[0].text
                        fore_name = author[1].text
                    author_info.append(author_id)
                    author_info.append(fore_name)
                    author_info.append(last_name)
                    authors_list.append(tuple(author_info))
            
        else:    
            for y in article:
                if y.tag=="AuthorList":
                    author_list = y.getchildren()
            #author_list = article[3].findall('./')
            print(author_list)
            for j in range(len(author_list)):
                last_name=""
                fore_name=""
                author_id = pmid + '_' + str(j)
                author = author_list[j].findall('./')
                print(author)
                author_info = []
                print(str(len(author))+"  length of author")
                print(author[0].text)
                if author[0].tag != 'LastName':
                    continue
                else:
                    if(len(author)==1):
                        last_name = author[0].text
                    else:
                        last_name = author[0].text
                        fore_name = author[1].text
                    author_info.append(author_id)
                    author_info.append(fore_name)
                    author_info.append(last_name)
                    authors_list.append(tuple(author_info))
    # print(authors_list)
    if len(authors_list) < 1:
        continue
    else:
        authors_records = ", ".join(["%s"] * len(authors_list))
        authors_data_insert_query = f"INSERT INTO author (authorid, fore_name,last_name) VALUES {authors_records}"
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(authors_data_insert_query, authors_list)
    
        


# In[ ]:


# Inserting data into Journal table (journal_issn, publication date, journal title, publication_type, author_id, volume)
from datetime import datetime

from tqdm import tqdm
for i in tqdm(range(len(root.find('.')))):
    pubmed_article = root.find('.')[i]
    citation = pubmed_article.findall("./MedlineCitation/")
    pmid = citation[0].text
    authors_list = []
    if citation[1].tag != 'DateCompleted':
        date_completed = '21/10/19 00:00:00'
    else:
        date = citation[1].findall('./')
        year, month, day = date[0].text, date[1].text, date[2].text
        date_completed = str(day)+'/'+str(month)+'/'+str(year[2:])+' 00:00:00'
    if len(citation) < 7:
        if citation[2].tag == 'Article':
            article = citation[2].findall('./')
            if len(article) == 7:
                
                abstract = article[3].findall('./')
                abstract_text = abstract.findall('./')
            else:
                abstract = article[4].findall('./')
            journal = article[0].findall('./')
            journal_issue = journal[1].findall('./')
            #journal_title = journal[2].text
            if (journal_title=="L'union medicale du Canada"):
                journal_title="L union medicale du Canada";
            volume = journal_issue[0].text
            if len(journal_issue) < 3:
                pub_date = journal_issue[1].findall('./')
            else:
                pub_date = journal_issue[2].findall('./')
            if len(pub_date) > 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, pub_date[2].text
                publication_date = pub_date_year+pub_date_month+pub_date_day
            elif len(pub_date) == 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, '01'
                publication_date = pub_date_year+' '+pub_date_month+' '+pub_date_day
                # publication_date = datetime.strptime(publication_date,'%y %b %d').date()
                # print(publication_date)
            else:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text,'JAN', '1'
                publication_date = pub_date_year+pub_date_month+pub_date_day
            author_list = article[5].findall('./')
            # print(i, len(author_list), author_list)
            for j in range(len(author_list)):
                author_info = []
                author_id = pmid + '_' + str(j)
                author = author_list[j].findall('./')
                last_name = author[0].text
                fore_name = author[1].text
                author_info.append(author_id)
                author_info.append(last_name)
                author_info.append(fore_name)
                authors_list.append(tuple(author_info))
            journal_issn = journal[0].text
            journal_title = journal[2].text
            if (journal_title=="L'union medicale du Canada"):
                journal_title="L union medicale du Canada";
            # journal_issn = journal_issn.replace('-', '0')
            # journal_issn = journal_issn.replace('X', '0')
            # print(i, journal_issn, type(journal_issn))
            title = article[1].text.replace("'",'')
            if len(article) > 9:
                publication_type_list = article[8].findall('./')
                publication_type = publication_type_list[0].text
            elif len(article) < 8:
                publication_type_list = article[6].findall('./')
                publication_type = publication_type_list[0].text
            else:
                publication_type_list = article[7].findall('./')
                publication_type = publication_type_list[0].text
        else:
            article = citation[3].findall('./')
            abstract = article[4].findall('./')
            journal = article[0].findall('./')
            journal_issue = journal[1].findall('./')
            journal_title = journal[2].text
            if (journal_title=="L'union medicale du Canada"):
                journal_title="L union medicale du Canada";
            for x in journal:
                if x.tag=="Volume":
                    volume=x.text 
        
        
            if (len(journal_issue)==1):
                volume = journal_issue.text
            else:
                if (len(journal_issue)==0):
                    continue
                else:
                    volume = journal_issue[0].text
            
            
            if len(journal_issue) < 3:
                pub_date = journal_issue[1].findall('./')
            else:
                pub_date = journal_issue[2].findall('./')
            if len(pub_date) > 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, pub_date[2].text
                publication_date = pub_date_year+pub_date_month+pub_date_day
            elif len(pub_date) == 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, '01'
                publication_date = pub_date_year+pub_date_month+pub_date_day
            else:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text,'JAN', '01'
                publication_date = pub_date_year+pub_date_month+pub_date_day
            author_list = article[3].findall('./')
            for j in range(len(author_list)):
                author_info = []
                author_id = pmid + '_' + str(j)
                author = author_list[j].findall('./')
                if(len(author)==1):
                #print(author)
                    last_name = author[0].text
                    fore_name=""
                else:    
                
                
                    if author[0].tag != 'LastName':
                            continue
                    last_name = author[0].text
                    fore_name = author[1].text
                author_info.append(author_id)
                author_info.append(last_name)
                author_info.append(fore_name)
                authors_list.append(tuple(author_info))
            journal_issn = journal[0].text
            # journal_issn = journal_issn.replace('-', '0')
            # journal_issn = journal_issn.replace('X', '0')
            # print(i, journal_issn, type(journal_issn))
            title = article[1].text.replace("'",'')
            for i in article:
                if i.tag=="PublicationTypeList":
                    publication_type_list = i.findall('./')
                    publication_type = publication_type_list[0].text
    else:
        article = citation[3].findall('./')
        journal = article[0].findall('./')
        journal_issue = journal[1].findall('./')
        journal_title = journal[2].text
        if (journal_title=="L'union medicale du Canada"):
            journal_title="L union medicale du Canada";
        
        for x in journal_issue:
            if x.tag=="Volume":
                volume=x.text 
        
        
        
            
        
        if (len(journal_issue)==0):
            pass
                
        else:
                
                
                volume = journal_issue[0].text
                
        if len(journal_issue) < 3:
            for w in journal_issue:
                if w.tag=="PubDate":
                    pub_date = w.findall('./')
        
        if len(pub_date) > 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, pub_date[2].text
                publication_date = pub_date_year+pub_date_month+pub_date_day
        elif len(pub_date) == 2:
            pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, '1'
            publication_date = pub_date_year+pub_date_month+pub_date_day
        else:
            pub_date_year, pub_date_month, pub_date_day = pub_date[0].text,'JAN', '1'
            publication_date = pub_date_year+pub_date_month+pub_date_day
        for y in article:
            if y.tag=="AuthorList":
                author_list = y.getchildren()
        
        for j in range(len(author_list)):
            last_name=[]
            fore_name=[]
            author_id = pmid + '_' + str(j)
            author = author_list[j].findall('./')
            author_info = []
            if(len(author)==1):
                #print(author)
                last_name = author[0].text
                fore_name = " "
                
            else:    
                
                
                    
                
                    
                    if author[0].tag != 'LastName':
                            continue
                    else:
                        last_name = author[0].text
                        fore_name = author[1].text
                    author_info.append(author_id)
                    author_info.append(last_name)
                    author_info.append(fore_name)
                    authors_list.append(tuple(author_info))
        journal_issn = journal[0].text
        # journal_issn = journal_issn.replace('-', '0')
        # journal_issn = journal_issn.replace('X', '0')
        # print(i, journal_issn, type(journal_issn), int(journal_issn))
        title = article[1].text.replace("'",'')
        
        for i in article:
            if i.tag=="PublicationTypeList":
                publication_type_list = i.findall('./')
                publication_type = publication_type_list[0].text
        print(publication_type)
    date_completed = datetime.strptime(date_completed, '%d/%m/%y %H:%M:%S')
    #print(date_completed)
    if len(authors_list) < 1:
        continue
    else:
        
        journal_list = []
        journal_list.append(journal_issn)
        journal_list.append(volume)
        journal_list.append(str(date_completed))
        journal_list.append(journal_title)
        journal_list.append(publication_type)
        journal_list.append(authors_list[0][0])
        
        journal_list = tuple(journal_list)
    
        #print(i, journal_list)
        journal_records = ", ".join(["%s"] * len(journal_list))
        # print(journal_records)
       
        print(str(authors_list)+"  this is authors list")
        try:
            journal_data_insert_query = f"INSERT INTO Medical_jounal_info (issn,volume, publish_date, title, Publica_type, authorid ) VALUES {journal_list}"
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(journal_data_insert_query, journal_list)
        except (psycopg2.errors.UniqueViolation, psycopg2.errors.IntegrityError) as e:
            print(e)
        


# In[30]:


# Inserting data into medline citation table
import re
from lxml import etree

doc = etree.parse('pubmed22n0003.xml')

root = doc.getroot()
from tqdm import tqdm
for i in tqdm(range(len(root.find('.')))):
    pubmed_article = root.find('.')[i]
    citation = pubmed_article.findall("./MedlineCitation/")
    abstract_data="No data found"
    title="No title found--improper article"
    pmid = citation[0].text
    authors_list = []
    cleaned=[]
    journal_issn="no ISSN found"
    publication_date='2022-07-12'
                                            
    
    
    
    
    if citation[1].tag != 'DateCompleted':
        date_completed = '2022-07-12'
        publication_date='2022-07-12'
    else:
        date = citation[1].findall('./')
        year, month, day = date[0].text, date[1].text, date[2].text
        date_completed = year+month+day
    if len(citation) < 7:
        if citation[2].tag == 'Article':
            article = citation[2].findall('./')
            if len(article) == 7:
                abstract = article[3].findall('./')
                abstract_text = abstract[0].findall('./')
                abstract_data=abstract_text[0]
                
                
                break
                
            else:
                abstract = article[4].findall('./')
                abstract_text=abstract[0].findall('./')
                abstract_data=abstract_text[0]
                
                break
            journal = article[0].findall('./')
            journal_issue = journal[1].findall('./')
            volume = journal_issue[0].text
            if len(journal_issue) < 3:
                pub_date = journal_issue[1].findall('./')
            else:
                pub_date = journal_issue[2].findall('./')
            if len(pub_date) > 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, pub_date[2].text
                publication_date = pub_date_year+pub_date_month+pub_date_day
            elif len(pub_date) == 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, ''
                publication_date = pub_date_year+pub_date_month+pub_date_day
            else:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text,'', ''
                publication_date = pub_date_year+pub_date_month+pub_date_day
            author_list = article[5].findall('./')
            # print(i, len(author_list), author_list)
            for j in range(len(author_list)):
                author_info = []
                author_id = pmid + '_' + str(j)
                author = author_list[j].findall('./')
                last_name = author[0].text
                fore_name = author[1].text
                author_info.append(author_id)
                author_info.append(last_name)
                author_info.append(fore_name)
                authors_list.append(tuple(author_info))
            if journal[0].text=="\n            ":
                journal_issn="no ISSN found"
            else:
                journal_issn = journal[0].text
            # journal_issn = journal_issn.replace('-', '0')
            # journal_issn = journal_issn.replace('X', '0')
            # print(i, journal_issn, type(journal_issn))
            if aeticle[1].text.replace("'",'')==' ':
                title="No title found"
            else:
                title = article[1].text.replace("'",'')
                title = title.replace("[",'')
                title = title.replace("]",'')
            
            if len(article) > 9:
                publication_type_list = article[8].findall('./')
                publication_type = publication_type_list[0].text
            elif len(article) < 8:
                publication_type_list = article[6].findall('./')
                publication_type = publication_type_list[0].text
            else:
                publication_type_list = article[7].findall('./')
                publication_type = publication_type_list[0].text
        else:
            string1=""
            
            article = citation[3].findall('./')
            abstract = article[4].findall('./')
            
                
            for i in article:
                if i.tag=="Abstract":
                    abstract_text=i.getchildren()
                    abstract_data=abstract_text.text
                    
                    break
            journal = article[0].findall('./')
#             print(journal)
            for i in journal:
                
                if i.tag=="JournalIssue":
                    
                    journal_issue = i.getchildren()
            for i in journal_issue:
                if i.tag=="Volume":
                
                    volume = journal_issue[0].text
            xix=len(journal_issue)
            if (xix)<2:
                
                pub_date = journal_issue
            elif xix < 3:
                pub_date = journal_issue[1].findall('./')
            else:
                pub_date = journal_issue[2].findall('./')
            if len(pub_date) > 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, pub_date[2].text
                publication_date = pub_date_year+pub_date_month+pub_date_day
            elif len(pub_date) == 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, ''
                publication_date = pub_date_year+pub_date_month+pub_date_day
            else:
                xixi=pub_date[0].text
                xixu=(string1.join(xixi))
                #xixt = str(xixi).split(' ')
                xio = re.findall('[0-9]+', xixu)
                #print(xixt[0])
                #fig=str(xixu[0])
                
                yxi=xixu.strip( *xixi[0] )
                pub_date_year = xixi[0]
                pub_date_month= yxi
                publication_date = pub_date_year+pub_date_month+'  1'
                
                
                
            for y in article:
                if y.tag=="AuthorList":
                    author_list = y.getchildren()
                if y.tag=="Abstract":
                    abstract_text=y.getchildren()
                    abstract_data=abstract_text.text
                    
            for j in range(len(author_list)):
                author_info = []
                author_id = pmid + '_' + str(j)
                author = author_list[j].findall('./')
                if len(author)>1:
                    last_name = author[0].text
                    fore_name = author[1].text
                else:
                    lastname=author[0].text
                    forename=""
                author_info.append(author_id)
                author_info.append(last_name)
                author_info.append(fore_name)
                authors_list.append(tuple(author_info))
            if journal[0].text=="\n            ":
                journal_issn="no ISSN found"
            else:
                journal_issn = journal[0].text
            # journal_issn = journal_issn.replace('-', '0')
            # journal_issn = journal_issn.replace('X', '0')
            # print(i, journal_issn, type(journal_issn))
            if article[1].text.replace("'",'')==' ':
                title="No title found" 
            else:
                title = article[1].text.replace("'",'')
                title = title.replace("[",'')
                title = title.replace("]",'')
            if len(article) > 9:
                publication_type_list = article[8].findall('./')
                
                publication_type = publication_type_list[0].text
                #print(publication_type_list)
            elif len(article) < 8 :
                #print(article)
                for i in article:
                    if i.tag=="Abstract":
                        abstract_text=i.getchildren()
                        abstract_data=abstract_text.text
                        
                    if i.tag=="PublicationTypeList":
                        publication_type_list=i.findall('./')
                        publication_type=publication_type_list[0].text
                #publication_type_list = article[6].findall('./')
                
                #publication_type = publication_type_list[0].text
                #print(publication_type_list)
           
            else:
                publication_type_list = article[7].findall('./')
                
                publication_type = publication_type_list[0].text
                #print(publication_type_list)
    else:
        
        article = citation[3].findall('./')
        
        journal = article[0].findall('./')
        
        #journal_issue = journal[1].findall('./')
        for i in journal:
                
                if i.tag=="JournalIssue":
                    
                    journal_issue = i.getchildren()
        #print(journal)
        for x in journal:
            if x.tag=="Volume":
                volume=x.text
        if (len(journal_issue)==1):
            
            volume=journal_issue[0].text
        else:
            if (len(journal_issue)==0):
                continue
            else:
                volume=journal_issue[0].text
        #volume = journal_issue[0].text
        if (len(journal_issue) < 3) and (len(journal_issue) > 1):
            
            pub_date = journal_issue[1].findall('./')
        elif (len(journal_issue) < 2):
            pub_date = journal_issue[0].findall('./')
        else:
            pub_date = journal_issue[2].findall('./')
        if len(pub_date) > 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, pub_date[2].text
                publication_date = pub_date_year+pub_date_month+pub_date_day
        elif len(pub_date) == 2:
            pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, ''
            publication_date = pub_date_year+pub_date_month+pub_date_day
        else:
            pub_date_year, pub_date_month, pub_date_day = pub_date[0].text,'', ''
            publication_date = pub_date_year+pub_date_month+pub_date_day
        #if (len(article)>7):    
            #author_list=article[3].findall('./')
        if article[3].tag=='Abstract':
            #author_list=article[4].findall('./')
            abstract_text=article[3].getchildren()
            abstract_data=abstract_text[0].text
            
            
        for y in article:
            if y.tag=="AuthorList":
                    author_list = y.getchildren()
            if y.tag=="Abstract":
                abstract_text=y.getchildren()
                abstract_data=abstract_text[0].text
                
                break
        
        for j in range(len(author_list)):
            last_name=""
            fore_name=""
            author_id = pmid + '_' + str(j)
            author = author_list[j].findall('./')
            author_info = []
            
            
            if(len(author)==1):
                #print(author)
                last_name = author[0].text
                fore_name = " "
                
            else: 
                
                
                if author[0].tag != 'LastName':
                    continue
                
                    last_name = author[0].text
                    fore_name = author[1].text
                author_info.append(author_id)
                author_info.append(last_name)
                author_info.append(fore_name)
                authors_list.append(tuple(author_info))
        if journal[0].text=="\n            ":
            journal_issn="no ISSN found"
        else:
            
            journal_issn = journal[0].text
        # journal_issn = journal_issn.replace('-', '0')
        # journal_issn = journal_issn.replace('X', '0')
        # print(i, journal_issn, type(journal_issn), int(journal_issn))
        if article[1].text.replace("'",'')==' ':
            title="No title found"
        else:
            title = article[1].text.replace("'",'')
            title = title.replace("[",'')
            title = title.replace("]",'')
        #print(len(article))
        for i in article:
            if i.tag=="Abstract":
                abstract_text=i.getchildren()
                abstract_data=abstract_text[0].text
                
                break
            if i.tag=="PublicationTypeList":
                publication_type_list=i.findall('./')
                publication_type=publication_type_list[0].text
    if len(authors_list) < 1:
        continue
    else:
        if journal_issn =="no ISSN found":
            
            continue
        else:  
        
            try:
                cleaned=abstract_data
                cleaned=cleaned[:250]
                citation_list = []
                citation_list.append(int(pmid))
                citation_list.append(date_completed)
                citation_list.append(journal_issn)
                citation_list.append(title)
                citation_list.append(cleaned)

                citation_list = tuple(citation_list)

                citation_records = ", ".join(["%s"] * len(citation_list))
                # print(journal_records)
                citation_data_insert_query = f"INSERT INTO citation (pmid,date_completed,issn, article_title,  abstract_text) VALUES {citation_list}"
                connection.autocommit = True
                cursor = connection.cursor()
                cursor.execute(citation_data_insert_query, citation_list)
            except:
                
                print(citation_list) 
                


# In[ ]:





# In[54]:


# Inserting data into citation_author table
from datetime import datetime

from tqdm import tqdm
for i in tqdm(range(len(root.find('.')))):
    pubmed_article = root.find('.')[i]
    citation = pubmed_article.findall("./MedlineCitation/")
    pmid = citation[0].text
    authors_list = []
    if citation[1].tag != 'DateCompleted':
        date_completed = '21/10/19 00:00:00'
    else:
        date = citation[1].findall('./')
        year, month, day = date[0].text, date[1].text, date[2].text
        date_completed = str(day)+'/'+str(month)+'/'+str(year[2:])+' 00:00:00'
    if len(citation) < 7:
        if citation[2].tag == 'Article':
            article = citation[2].findall('./')
            if len(article) == 7:
                
                abstract = article[3].findall('./')
                abstract_text = abstract.findall('./')
            else:
                abstract = article[4].findall('./')
            journal = article[0].findall('./')
            journal_issue = journal[1].findall('./')
            #journal_title = journal[2].text
            if (journal_title=="L'union medicale du Canada"):
                journal_title="L union medicale du Canada";
            volume = journal_issue[0].text
            if len(journal_issue) < 3:
                pub_date = journal_issue[1].findall('./')
            else:
                pub_date = journal_issue[2].findall('./')
            if len(pub_date) > 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, pub_date[2].text
                publication_date = pub_date_year+pub_date_month+pub_date_day
            elif len(pub_date) == 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, '01'
                publication_date = pub_date_year+' '+pub_date_month+' '+pub_date_day
                # publication_date = datetime.strptime(publication_date,'%y %b %d').date()
                # print(publication_date)
            else:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text,'JAN', '1'
                publication_date = pub_date_year+pub_date_month+pub_date_day
            author_list = article[5].findall('./')
            # print(i, len(author_list), author_list)
            for j in range(len(author_list)):
                author_info = []
                author_id = pmid + '_' + str(j)
                author = author_list[j].findall('./')
                last_name = author[0].text
                fore_name = author[1].text
                author_info.append(author_id)
                author_info.append(last_name)
                author_info.append(fore_name)
                authors_list.append(tuple(author_info))
            journal_issn = journal[0].text
            journal_title = journal[2].text
            if (journal_title=="L'union medicale du Canada"):
                journal_title="L union medicale du Canada";
            # journal_issn = journal_issn.replace('-', '0')
            # journal_issn = journal_issn.replace('X', '0')
            # print(i, journal_issn, type(journal_issn))
            title = article[1].text.replace("'",'')
            if len(article) > 9:
                publication_type_list = article[8].findall('./')
                publication_type = publication_type_list[0].text
            elif len(article) < 8:
                publication_type_list = article[6].findall('./')
                publication_type = publication_type_list[0].text
            else:
                publication_type_list = article[7].findall('./')
                publication_type = publication_type_list[0].text
        else:
            article = citation[3].findall('./')
            abstract = article[4].findall('./')
            journal = article[0].findall('./')
            journal_issue = journal[1].findall('./')
            journal_title = journal[2].text
            if (journal_title=="L'union medicale du Canada"):
                journal_title="L union medicale du Canada";
            for x in journal:
                if x.tag=="Volume":
                    volume=x.text 
        
        
            if (len(journal_issue)==1):
                volume = journal_issue.text
            else:
                if (len(journal_issue)==0):
                    continue
                else:
                    volume = journal_issue[0].text
            
            
            if len(journal_issue) < 3:
                pub_date = journal_issue[1].findall('./')
            else:
                pub_date = journal_issue[2].findall('./')
            if len(pub_date) > 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, pub_date[2].text
                publication_date = pub_date_year+pub_date_month+pub_date_day
            elif len(pub_date) == 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, '01'
                publication_date = pub_date_year+pub_date_month+pub_date_day
            else:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text,'JAN', '01'
                publication_date = pub_date_year+pub_date_month+pub_date_day
            author_list = article[3].findall('./')
            for j in range(len(author_list)):
                author_info = []
                author_id = pmid + '_' + str(j)
                author = author_list[j].findall('./')
                if(len(author)==1):
                #print(author)
                    last_name = author[0].text
                    fore_name=""
                else:    
                
                
                    if author[0].tag != 'LastName':
                            continue
                    last_name = author[0].text
                    fore_name = author[1].text
                author_info.append(author_id)
                author_info.append(last_name)
                author_info.append(fore_name)
                authors_list.append(tuple(author_info))
            journal_issn = journal[0].text
            # journal_issn = journal_issn.replace('-', '0')
            # journal_issn = journal_issn.replace('X', '0')
            # print(i, journal_issn, type(journal_issn))
            title = article[1].text.replace("'",'')
            for i in article:
                if i.tag=="PublicationTypeList":
                    publication_type_list = i.findall('./')
                    publication_type = publication_type_list[0].text
    else:
        article = citation[3].findall('./')
        journal = article[0].findall('./')
        journal_issue = journal[1].findall('./')
        journal_title = journal[2].text
        if (journal_title=="L'union medicale du Canada"):
            journal_title="L union medicale du Canada";
        
        for x in journal_issue:
            if x.tag=="Volume":
                volume=x.text 
        
        
        
            
        
        if (len(journal_issue)==0):
            pass
                
        else:
                
                
                volume = journal_issue[0].text
                
        if len(journal_issue) < 3:
            for w in journal_issue:
                if w.tag=="PubDate":
                    pub_date = w.findall('./')
        
        if len(pub_date) > 2:
                pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, pub_date[2].text
                publication_date = pub_date_year+pub_date_month+pub_date_day
        elif len(pub_date) == 2:
            pub_date_year, pub_date_month, pub_date_day = pub_date[0].text, pub_date[1].text, '1'
            publication_date = pub_date_year+pub_date_month+pub_date_day
        else:
            pub_date_year, pub_date_month, pub_date_day = pub_date[0].text,'JAN', '1'
            publication_date = pub_date_year+pub_date_month+pub_date_day
        for y in article:
            if y.tag=="AuthorList":
                author_list = y.getchildren()
        
        for j in range(len(author_list)):
            last_name=[]
            fore_name=[]
            author_id = pmid + '_' + str(j)
            author = author_list[j].findall('./')
            author_info = []
            if(len(author)==1):
                #print(author)
                last_name = author[0].text
                fore_name = " "
                
            else:    
                
                
                    
                
                    
                    if author[0].tag != 'LastName':
                            continue
                    else:
                        last_name = author[0].text
                        fore_name = author[1].text
                    author_info.append(author_id)
                    author_info.append(last_name)
                    author_info.append(fore_name)
                    authors_list.append(tuple(author_info))
        journal_issn = journal[0].text
        # journal_issn = journal_issn.replace('-', '0')
        # journal_issn = journal_issn.replace('X', '0')
        # print(i, journal_issn, type(journal_issn), int(journal_issn))
        title = article[1].text.replace("'",'')
        
        for i in article:
            if i.tag=="PublicationTypeList":
                publication_type_list = i.findall('./')
                publication_type = publication_type_list[0].text
        print(publication_type)
    date_completed = datetime.strptime(date_completed, '%d/%m/%y %H:%M:%S')
    #print(date_completed)

     
        
           
           


    if len(authors_list) < 1:
        continue
    else:
        cit_auth_list = []
        cit_auth_list.append(int(pmid))
        cit_auth_list.append(author_id)
        cit_auth_list = tuple(cit_auth_list)
        try:
            cit_auth_data_insert_query = f"INSERT INTO cit_author (pmid, authorid) VALUES {cit_auth_list}"
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(cit_auth_data_insert_query, cit_auth_list)
        except (psycopg2.errors.UniqueViolation, psycopg2.errors.IntegrityError) as e:
            print(e)


# In[41]:


# Inserting data into Meshdata table
from tqdm import tqdm
for i in tqdm(range(len(root.find('.')))):
    pubmed_article = root.find('.')[i]
    
    
    medline_citation = pubmed_article.findall("./MedlineCitation/")
    pmid = medline_citation[0].text
    mesh_list = []
    if len(medline_citation) < 7:
        continue
    elif len(medline_citation) == 8:
        if medline_citation[-1].tag == 'MeshHeadingList':
            Meshdata_list = medline_citation[-1].findall('./')
            for k in range(len(Meshdata_list)):
                mesh_id_list = []
                descriptor_name = Meshdata_list[k][0].text.replace('%','')
                mesh_id = Meshdata_list[k][0].get('UI')
                mesh_id_list.append(mesh_id)
                mesh_id_list.append(descriptor_name)
                mesh_list.append(tuple(mesh_id_list))
        else:
            Meshdata_list = medline_citation[-2].findall('./')
            for k in range(len(Meshdata_list)):
                mesh_id_list = []
                descriptor_name = Meshdata_list[k][0].text.replace('%','')
                mesh_id = Meshdata_list[k][0].get('UI')
                mesh_id_list.append(mesh_id)
                mesh_id_list.append(descriptor_name)
                mesh_list.append(tuple(mesh_id_list))
    elif len(medline_citation) == 9:
        if medline_citation[-1].tag == 'MeshHeadingList':
            Meshdata_list = medline_citation[-1].findall('./')
            for k in range(len(Meshdata_list)):
                mesh_id_list = []
                descriptor_name = Meshdata_list[k][0].text.replace('%','')
                mesh_id = Meshdata_list[k][0].get('UI')
                mesh_id_list.append(mesh_id)
                mesh_id_list.append(descriptor_name)
                mesh_list.append(tuple(mesh_id_list))
        else:
            Meshdata_list = medline_citation[-2].findall('./')
            mesh_list = []
            for k in range(len(Meshdata_list)):
                mesh_id_list = []
                descriptor_name = Meshdata_list[k][0].text.replace('%','')
                mesh_id = Meshdata_list[k][0].get('UI')
                mesh_id_list.append(mesh_id)
                mesh_id_list.append(descriptor_name)
                mesh_list.append(tuple(mesh_id_list))
    for j in range(len(mesh_list)):
        try:
            mesh_data_insert_query = f"INSERT INTO Meshdata (msid, Description) VALUES {mesh_list[j]}"
            connection.autocommit = True
            cursor = connection.cursor()
            print(mesh_list[j])
            cursor.execute(mesh_data_insert_query, mesh_list[j])
        except psycopg2.errors.UniqueViolation as e:
            print('same value occured')


# In[49]:


# Inserting data into cit_ Meshdata table
from tqdm import tqdm
for i in tqdm(range(len(root.find('.')))):
    pubmed_article = root.find('.')[i]
    medline_citation = pubmed_article.findall("./MedlineCitation/")
    pmid = medline_citation[0].text
    mesh_list = []
    if len(medline_citation) < 7:
        continue
    elif len(medline_citation) == 8:
        if medline_citation[-1].tag == 'MeshHeadingList':
            mesh_heading_list = medline_citation[-1].findall('./')
            for k in range(len(mesh_heading_list)):
                mesh_id_list = []
                descriptor_name = mesh_heading_list[k][0].text.replace('%','')
                mesh_id = mesh_heading_list[k][0].get('UI')
                mesh_id_list.append(pmid)
                mesh_id_list.append(mesh_id)
                mesh_list.append(tuple(mesh_id_list))
        else:
            mesh_heading_list = medline_citation[-2].findall('./')
            for k in range(len(mesh_heading_list)):
                mesh_id_list = []
                descriptor_name = mesh_heading_list[k][0].text.replace('%','')
                mesh_id = mesh_heading_list[k][0].get('UI')
                mesh_id_list.append(pmid)
                mesh_id_list.append(mesh_id)
                mesh_list.append(tuple(mesh_id_list))
    elif len(medline_citation) == 9:
        if medline_citation[-1].tag == 'MeshHeadingList':
            mesh_heading_list = medline_citation[-1].findall('./')
            for k in range(len(mesh_heading_list)):
                mesh_id_list = []
                descriptor_name = mesh_heading_list[k][0].text.replace('%','')
                mesh_id = mesh_heading_list[k][0].get('UI')
                mesh_id_list.append(pmid)
                mesh_id_list.append(mesh_id)
                mesh_list.append(tuple(mesh_id_list))
        else:
            mesh_heading_list = medline_citation[-2].findall('./')
            mesh_list = []
            for k in range(len(mesh_heading_list)):
                mesh_id_list = []
                descriptor_name = mesh_heading_list[k][0].text.replace('%','')
                mesh_id = mesh_heading_list[k][0].get('UI')
                mesh_id_list.append(pmid)
                mesh_id_list.append(mesh_id)
                mesh_list.append(tuple(mesh_id_list))
    for j in range(len(mesh_list)):
        try:
            mesh_data_insert_query = f"INSERT INTO cit_mesh (pmid, msid) VALUES {mesh_list[j]}"
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(mesh_data_insert_query, mesh_list[j])
        except (psycopg2.errors.UniqueViolation, psycopg2.errors.IntegrityError) as e:
            print('same value occured')


# In[28]:


# Inserting data into references table
from tqdm import tqdm
for i in tqdm(range(len(root.find('.')))):
    pubmed_article = root.find('.')[i]
    medline_citation = pubmed_article.findall("./MedlineCitation/")
    pmid = medline_citation[0].text
    pubmed_data = pubmed_article.findall("./PubmedData/")
    reference_table_list = []
    if len(pubmed_data) == 4:
        reference_list = pubmed_data[3].findall('./')
        for k in range(len(reference_list)):
            row_list = []
            reference = reference_list[k].findall('./')
            citation = reference[0].text
            article_id_list = reference[1].findall('./')
            article_id = article_id_list[0].text
            row_list.append(article_id)
            row_list.append(pmid)
            row_list.append(citation)
            
            reference_table_list.append(tuple(row_list))
            print(i, citation, article_id,citation)
    else:
        continue
    for j in range(len(reference_table_list)):
         #print(mesh_list[j])
        try:
            references_data_insert_query = f"INSERT INTO refe ( articleid,pmid, citation) VALUES {reference_table_list[j]}"
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(references_data_insert_query, reference_table_list[j])
        except (psycopg2.errors.UniqueViolation, psycopg2.errors.IntegrityError) as e:
            print('same value occured')

    


# In[29]:


# Inserting data into chemical_list table
from tqdm import tqdm
for i in tqdm(range(len(root.find('.')))):
    pubmed_article = root.find('.')[i]
    medline_citation = pubmed_article.findall("./MedlineCitation/")
    pmid = medline_citation[0].text
    chemical_data_list = []
    if len(medline_citation) > 7:
        chemical_list = medline_citation[5].findall('./')
        for k in range(len(chemical_list)):
            row_list = []
            chemical = chemical_list[k].findall('./')
            registry_no = chemical[0].text
            name_of_substance = chemical[1].text.replace('%','')
            name_of_substance=name_of_substance[:30]
            
            row_list.append(name_of_substance)
            row_list.append(registry_no)
            row_list.append(pmid)
            chemical_data_list.append(tuple(row_list))
    else:
        continue
    
    for j in range(len(chemical_data_list)):
        #print(chemical_data_list[j])
        try:
            chemical_data_insert_query = f"INSERT INTO chemical_list ( substance_name, registernum,pmid) VALUES {chemical_data_list[j]}"
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(chemical_data_insert_query, chemical_data_list[j])
        except (psycopg2.errors.UniqueViolation, psycopg2.errors.IntegrityError) as e:
            print('same value occured')


# In[ ]:





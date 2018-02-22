import re
import html
from html.parser import HTMLParser
from xml.etree import ElementTree as ET
from pathlib import Path


class StackExchange(object):
    
    class __MLStripper__(HTMLParser):
        """
        HTML Parser that receives a string with HTML tags, strips out tags. get_data() will return a string devoid of HTML tags.
        
        """
        def __init__(self):
            super().__init__()
            self.reset()
            self.strict = False
            self.convert_charrefs= True
            self.fed = []
        def handle_data(self, d):
            self.fed.append(d)
        def get_data(self):
            return ''.join(self.fed)
    
    def __init__(self, file, community=None, content_type='post_title', remove_html=True):
        """
        A Prodigy compliant corpus loader that reads a StackExchange xml file and yields a stream of text in dictionary format.
        
        :param file: string path name to xml file
        :param community: string, name of stackexchange community
        :param content_type: string, select the type of text to return: post_title, post_body, comments
        :param remove_html: Boolean, Remove or keep HTML tags in the text
        """
        
        # Check that the path actually exists and is a recognizable XML file
        se_file = Path(file).absolute()
        assert (se_file.exists()), "Cannot find file. Please check the path name and try again"
        assert (se_file.suffix == '.xml'), "File does not end in '.xml'. Please check the path name and try again"
        self.file = se_file
        
        #If user doesn't supply the community assume a normal unziping process occured and the community is the parent directory
        if not community:
             community = self.file.parent.parts[-1]
        self.community = community
        
        # Acceptable types of StackExchange text content
        self._TYPES = ['post_title', 'post_body', 'post_both', 'comments']
        assert (content_type.lower() in self._TYPES), "Content Type not understood. Acceptable types include {}".format(self._TYPES)
        self.content_type = content_type
        
        assert(remove_html in [True, False]), "remove_html must be either True or False"
        self.remove_html = remove_html
        # Lazily load the xml file, puts a blocking lock on the file
        self.tree = ET.iterparse(self.file.as_posix(), events=['start', 'end'])
    
    def _parse_tags(self, tags):
        """
        Parse the Tags attribute of a row in a StackExchange Posts xml file
        
        :param tags: string, tags formatted between <>
        :returns: List of tags or None
        """
        
        if tags == '':
            return None
        else:
            t = re.compile('<(.+?)>')
            m = t.findall(tags)
            return m
        
    def __iter__(self):
        
        # Iterate through the file and yield the text
        for _, child in self.tree:

            # Start of file, check that the file matches the expected content_type
            if _ == 'start' and child.tag != 'row':
                if self.content_type in self._TYPES[:3]:
                    assert(child.tag == 'posts'), "Input file is not a StackExchange Posts.xml file. Please check the path name and try again"

                else:
                    assert(child.tag == 'comments'), "Input file is not a StackExchange Comments.xml file. Please check the path name and try again"

            else:   
                 # Assemble the prodigy stream compliant dictonary object
                info = {"meta": {"source": "StackExchange", "Community": self.community, "type": self.file.stem}}

                atb = child.attrib
                if self.content_type in self._TYPES[:3]:
                    title = atb.get('Title', None)
                    body = atb.get('Body', None)
                    tags = atb.get('Tags', None)

                    if self.content_type == 'post_both':
                        if title and body:
                            text =  title + '\n' + body 
                        elif body and not title:
                            text = body
                        elif title and not body:
                            text = title
                        else:
                            text = None

                    elif self.content_type == 'post_title':
                        text = title

                    elif self.content_type == 'post_body':
                        text = body

                elif self.content_type == 'comments':
                    tags = None
                    text = atb.get('Text', None)

                else:
                    tags = None
                    text = None

                # Check to see if valid text was found, if not, skip to the next xml child element
                if not text:
                    continue

                else:
                    # unescape HTML encoding and remove html tags
                    text = html.unescape(text)
                    if self.remove_html:
                        #HTML Stripper
                        stripper = self.__MLStripper__()
                        stripper.feed(text)
                        text = stripper.get_data()

                    # Append the text and additional metadata to the stream dictionary
                    info['text'] = text
                    info['meta']['ID'] = atb['Id']

                    if tags != None:
                        tags = self._parse_tags(tags)
                    info['meta']['Tags'] = tags

                    #yield the dictionary
                    yield info

            # clear the child from memory before moving to the next child element
            child.clear()

  
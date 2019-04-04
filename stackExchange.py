import re
import html
from html.parser import HTMLParser
from xml.etree import ElementTree as ET
from pathlib import Path
from prodigy import log


class StackExchange(object):
    
    class __MLStripper__(HTMLParser):
        """
        HTML Parser that receives a string with HTML tags, strips out tags. get_data() will return a string devoid of HTML tags.
        
        """
        def __init__(self, convert_charrefs=True):
            super().__init__()
            self.reset()
            self.strict = False
            self.convert_charrefs= True
            self.fed = []

        def handle_data(self, d):
            self.fed.append(d)

        def get_data(self):
            return ''.join(self.fed)

        def error(self, message):
            pass
    
    def __init__(self, file, community=None, content_type='post_title', newlines=True, onlytags=[]):
        """
        A Prodigy compliant corpus loader that reads a StackExchange xml file and yields a stream of text in dictionary format.
        
        :param file: string path name to xml file
        :param community: string, name of stackexchange community
        :param content_type: string, select the type of text to return: post_title, post_body, comments
        :param newlines: Boolean, If True, keep newlines in text, if False, replace newlines with space.
        :param onlytags: Only return posts which contain one or more of the provided tags
        """
        
        # Check that the path actually exists and is a recognizable XML file
        se_file = Path(file).absolute()
        # There is no easy way to stream into memory, Archives and files compressed using 7zip's LZMA format
        # So we must rely on the user to unzip the archive before using the file.
        assert (se_file.exists()), "Cannot find file. Please check the path name and try again"
        assert (se_file.suffix == '.xml'), "File does not end in '.xml'. Please check the path name and try again"
        log('STREAM: {} file found'.format(se_file.as_posix()))
        self.file = se_file
        
        # Regex to find newlines
        self.newlines = newlines
        self.newline = re.compile(r'\n+')
        # If user doesn't supply the community assume a normal unziping process occured and
        # the community is the parent directory
        if not community:
             community = self.file.parent.parts[-1]
        self.community = community
        
        # Acceptable types of StackExchange text content
        self._TYPES = ['post_title', 'post_body', 'post_both', 'comments']
        assert (content_type.lower() in self._TYPES), "Content Type not understood. Acceptable types include {}".format(self._TYPES)
        self.content_type = content_type
        
        # Lazily load the xml file, puts a blocking lock on the file
        # TODO: Change to 'end' events only
        self.tree = ET.iterparse(self.file.as_posix(), events=['start', 'end'])
        
        # To identify even more specific results a user can supply a StackExchange tag or tags.
        # Only Posts with one or more tags will be returned
        if type(onlytags) == str:
            onlytags = [onlytags]
        self.onlytags = onlytags
        
        # Keep a count of total and parsed rows
        self.total = 0
        self.parsed = 0
        
        # Keep track of Question tags for use by Answer Posts
        # Also keep a count of the number of expected answers and the number of seen answers
        self.parent_post_tags = {}

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
            
            self.total += 1
            log("STREAM: Fetching {} child element".format(self.total, child.attrib))
            # Start of file, check that the file matches the expected content_type
            if _ == 'start' and child.tag != 'row':
                if self.content_type in self._TYPES[:3]:
                    assert(child.tag == 'posts'), "Input file is not a StackExchange Posts.xml file. \
                    Please check the path name and try again"

                else:
                    assert(child.tag == 'comments'), "Input file is not a StackExchange Comments.xml file. \
                    Please check the path name and try again"

            else:   
                # Assemble the prodigy stream compliant dictonary object
                info = {"meta": {"source": "StackExchange", "Community": self.community, "type": self.file.stem}}

                atb = child.attrib
                # Fetch the necessary information
                if self.content_type in self._TYPES[:3]:
                    Id = atb.get('Id', None)
                    title = atb.get('Title', None)
                    body = atb.get('Body', None)
                    tags = atb.get('Tags', None)
                    if tags is not None:
                        tags = self._parse_tags(tags)
                    posttype = atb.get('PostTypeId', None)
                    create = atb.get('CreationDate', None)
                    answers = int(atb.get('AnswerCount', 0))
                    
                    # Preserve Tag information from Questions for reference by Answers
                    if posttype == "1":
                        if answers > 0:
                            self.parent_post_tags[Id] = {'tags': tags, 'answers': answers, 'seen': 0}
                    
                    # If this post is an answer, lookup the tags on the parent question        
                    elif posttype == "2":
                        parentid = atb.get("ParentId", None)
                        parent = self.parent_post_tags.get(parentid, None)
                        
                        if parent:
                            tags = parent.get('tags', None)
                        else:
                            tags = None
                        
                        # If a parent answer value is found, update the seen answer count and
                        # check if all answers have been seen
                        if parent:
                                                    
                            # TODO: Test this code to verify it works as intended
                            # Update the seen answer count
                            if parent['seen'] < parent['answers']:
                                parent['seen'] += 1

                            # Now check if we've seen all the answers,
                            # If so, then delete the Parent Id entry to free up memory
                            if parent['seen'] == parent['answers']:
                                del self.parent_post_tags[parentid]    
                            
                    
                    # If the user only wants text from Posts with specific stackExchange tags,
                    # only return content that matches. Naively iterates through the stream of Posts.
                    # It does not know if the tag actually exists.
                    # If the user supplies tags, and no tags match, skip this post
                    if self.onlytags and not tags:
                        continue
                    elif self.onlytags and not any([tag for tag in tags if tag in self.onlytags]):
                        continue
                    else:   
                        
                        if self.content_type == 'post_both':
                            if title and body:
                                text = title + '\n' + body
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
                    if self.onlytags:
                        print("Tags are currently only available for Posts. Skipping search for {}".format(self.onlytags))
                    text = atb.get('Text', None)

                else:
                    text = None

                # Check to see if valid text was found, if not, skip to the next xml child element
                if text is None:
                    continue

                else:
                    # unescape HTML encoding and remove html tags
                    # Preserve the original HTML
                    info['html'] = text

                    # TODO: Sometimes causes problems in the HTML stripper, disable for now, investigate later
                    #text = html.unescape(text)
                    
                    # HTML Stripper
                    stripper = self.__MLStripper__()
                    stripper.feed(text)
                    text = stripper.get_data()
                    
                    # Remove extra newlines or all newlines
                    if self.newlines:
                        cleantext = self.newline.sub(r'\n', text)
                    else:
                        cleantext = self.newline.sub(r' ', text)
                    
                    # Append the text and additional metadata to the stream dictionary
                    info['text'] = cleantext
                    info['meta']['ID'] = Id
                    info['meta']['Date'] = create
                    info['meta']['Tags'] = tags

                    # yield the dictionary
                    self.parsed += 1
                    log("STREAM: {p} of {t} XML child element parsed".format(p=self.parsed, t=self.total), info)
                    yield info

            # clear the child from memory before moving to the next child element
            child.clear()

  
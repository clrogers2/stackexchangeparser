import re
from html.parser import HTMLParser
from xml.etree import ElementTree as ET
from pathlib import Path
from prodigy import log
import requests
from bs4 import BeautifulSoup
import subprocess


class StackExchangeParser(object):
    
    class _TagStripper(HTMLParser):
        """
        HTML Parser that receives a string with HTML tags, strips out tags. get_data() will return a string devoid of HTML tags.
        
        """
        def __init__(self, convert_charrefs=True):
            super().__init__()
            self.reset()
            self.strict = False
            self.convert_charrefs = convert_charrefs
            self.fed = []

        def handle_data(self, d):
            self.fed.append(d)

        def get_data(self):
            return ''.join(self.fed)

        def error(self, message):
            pass
    
    def __init__(self, file, community=None, content_type='post_title', metadata='all',
                 newlines=True, onlytags=None):
        """
        A Prodigy compliant corpus loader that reads a StackExchange xml file (or list of community urls) and yields a
        stream of text in dictionary format.
        
        :param file: None or string path name to xml file. If None, read files from Archive.org using communities param.
        :param community: string, name of StackExchange community
        :param content_type: string, select the type of text to return: post_title, post_body, comments
        :param metadata: List of post metadata to include in output.
        :param newlines: Boolean, If True, keep newlines in text, if False, replace newlines with space.
        :param onlytags: Only return posts which contain one or more of the provided tags
        """
        
        # Check that the path actually exists and is a recognizable XML file
        self.URL = 'https://archive.org/download/stackexchange/'
        self.communities = self._get_community_names()

        # Regex to find newlines
        self.newlines = newlines
        self.newline = re.compile(r'\n+')

        # Acceptable types of StackExchange text content
        self._TYPES = ['post_title', 'post_body', 'post_both', 'comments']
        assert (content_type.lower() in self._TYPES), " Acceptable content_types include {}".format(self._TYPES)
        self.content_type = content_type

        if not file:
            self.community = self._verify_community_names(community)
            log('STREAM: Attempting to download {}'.format(self.community))
            file = self._download_community(self.community)
            log('STREAM: {} downloaded. Attempting to decompress Posts.xml file'.format(file))

        se_file = Path(file).absolute()

        # If user doesn't supply the community assume a normal unziping process occurred and
        # the community is the parent directory
        if not community:
            community = self.file.parent.parts[-1]
        self.community = self._verify_community_names(community)

        assert (se_file.exists()), "Cannot find file. Please check the path name and try again"
        assert (se_file.suffix == '.xml'), "File does not end in '.xml'. Please check the path name and try again"
        log('STREAM: {} file found'.format(se_file.as_posix()))
        self.file = se_file

        # Lazily load the xml file, puts a blocking lock on the file
        self.tree = ET.iterparse(self.file.as_posix(), events=['end'])
        
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
    
    def _get_community_names(self):
        URL = self.URL
        page = requests.get(URL)
        page_html = BeautifulSoup(page.text, 'lxml')
        div = page_html.find('div', class_='download-directory-listing')
        table = div.find('table', class_='directory-listing-table')
        body = table.find_all('a')
        coms = [row.text.replace(".7z", "") for row in body if row.text.endswith('7z')]
        return coms

    def _verify_community_names(self, com):

        assert isinstance(com, str), "Community name must be a string"
        if com not in self.communities:
            log("STREAM: {com} not found in online archive at {url}".format(com=com, url=self.URL))
        assert (com in self.communities), """StackExchange community--{com}--not found 
        in online archive at {url}""".format(com=com, url=self.URL)
        return com

    @staticmethod
    def _rename_and_extract_7zip( file_name):
        posts = 'Posts' in file_name
        comments = 'Comments' in file_name
        tags = 'Tags' in file_name
        com_name = file_name.split('.')[0]
        if posts or (not posts and not comments and not tags):
            output_name = '{}_Posts.xml'.format(com_name)
            subprocess.call('7z rn {file} Posts.xml {com}'.format(file=file_name, com=output_name))
            subprocess.call('7z e {file} -iy {com} -o .'.format(file=file_name, com=output_name))
            return './'+output_name
        elif comments:
            output_name = '{}_Comments.xml'.format(com_name)
            subprocess.call('7z rn {file} Comments.xml {com}'.format(file=file_name, com=output_name))
            subprocess.call('7z e {file} -iy {com} -o .'.format(file=file_name, com=output_name))
            return './' + output_name
        elif tags:
            output_name = '{}_Tags.xml'.format(com_name)
            subprocess.call('7z rn {file} Tags.xml {com}'.format(file=file_name, com=output_name))
            subprocess.call('7z e {file} -iy {com} -o .'.format(file=file_name, com=output_name))
            return './' + output_name
        else:
            return None

    def _generate_file_markers(self, file_obj, mem_size=50, mem_unit='MB', order='default'):
        ORDERS = ['default', 'beginning', 'ending', 'shuffle']
        UNITS = {'GB': 4**1024, 'MB': 3**1024, 'KB': 2**1024, 'B': 1}
        if order.lower() not in ORDERS:
            order = 'default'
        if mem_unit not in UNITS.keys():
            mem_unit = 'MB'

        _mem_size = mem_size * UNITS[mem_unit]
        pass

    def _download_community(self, community):
        url = self.URL + community + '.7z'
        local_filename = url.split('/')[-1]
        # NOTE the stream=True parameter below
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        # f.flush()
        return local_filename

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
            log("STREAM: Fetching {} child element".format(self.total))
            # Start of file, check that the file matches the expected content_type
            if child.tag != 'row':
                if self.content_type in self._TYPES[:3]:
                    assert(child.tag == 'posts'), "Input file is not a StackExchange Posts.xml file. \
                    Please check the path name and try again"

                else:
                    assert(child.tag == 'comments'), "Input file is not a StackExchange Comments.xml file. \
                    Please check the path name and try again"

            else:   
                # Assemble the prodigy stream compliant dictionary object
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
                    favorite = int(atb.get('FavoritCount', 0))
                    score = int(atb.get('Score', 0))
                    comments = int(atb.get('CommentCount', 0))
                    views = int(atb.get('ViewCount', 0))

                    
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
                    stripper = self._TagStripper()
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

  
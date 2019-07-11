import re
from html.parser import HTMLParser
from xml.etree import ElementTree as ET
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import subprocess
import time
try:
    from prodigy import log
except (ImportError, ModuleNotFoundError):
    from .utils.log import Log
    log = Log()
from .utils import find_program, capture_7zip_stdout


class StackExchangeParser(object):
    
    class _TagStripper(HTMLParser):
        """
        HTML Parser that receives a string with HTML tags, strips out tags. get_data() will return a string devoid of
        HTML tags.
        
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
    
    def __init__(self, file, community, proj_dir='.', resume_from=None, content_type='post_body', newlines=True,
                 onlytags=None):
        """
        A Prodigy compliant corpus loader that reads a StackExchange xml file (or list of community urls) and yields a
        stream of text in dictionary format.
        
        :param file: None or string path name to xml file. If None, read files from Archive.org using communities param.
            string can be comma delimited to pass in two files from the same community.
        :param community: string or None, name of StackExchange community. If None, will attempt to identify community
            from the file name(s) or the directory name. If unable to determine the community the parser will exit with
            a ValueError.
        :param resume_from: dictionary mapping of metadata tag and the value to use from which to resume parsing
            Id: The max Post Id for this community from previous parses. This will NOT re-parse posts that have been
                edited since the last parse was run.
            Date: The date of the archive.org data dump from previous parse. This WILL re-parse posts that have changed
                since the last parse was run.
        :param content_type: string, select the type of text to return.
            post_title: Use the Posts.xml file and set 'text' to the post title.
            post_body: Use the Posts.xml file and set 'text' to the post body
            post_both: Use the Posts.xml file and set 'text' to BOTH the post title and the post body.
            all_text: Use the Posts.xml and Comments.xml files to set 'text' to BOTH title and body
                for BOTH posts and comments.
            comments_body: Use the Comments.xml file and set 'text' to the comment body.
            comments_both: Use the Comments.xml file and set 'text' to BOTH the parent title and comment body
        :param newlines: Boolean, If True, keep newlines in text, if False, replace newlines with space.
        :param onlytags: Only return posts which contain one or more of the provided tags
        """

        self.proj_dir = Path(proj_dir).absolute()
        if not self.proj_dir.exists():
            self.proj_dir.mkdir(parents=True)

        self.iter = iter(self)
        if file:
            file = file.split(',')  # If a single file is passed in, it will be placed into a list
            if len(file) == 1:  # Only one file was passed in, so remove it from the list before testing
                file = file[0]
        self._resume_keys = ['Id', 'Date']
        if resume_from:
            assert([*resume_from][0] in self._resume_keys)
        self.resume_from = resume_from

        # Check that the path actually exists and is a recognizable XML file
        self.URL = 'https://archive.org/download/stackexchange/'
        self.communities, self.latest_data_date = self._get_community_names()

        # Regex to find newlines
        self.newlines = newlines
        self.newline = re.compile(r'\n+')

        # Acceptable types of StackExchange text content
        self._TYPES = ['post_title', 'post_body', 'post_both', 'all_text', 'comments_both', 'comments_body', 'tags']
        assert (content_type.lower() in self._TYPES), " Acceptable content_types include {}".format(self._TYPES)
        self.content_type = content_type

        if 'post' in self.content_type:
            _name = 'Posts'
            _ = ''
        elif 'comments' in self.content_type:
            _name = 'Comments'
            _ = ''

        elif 'tags' in self.content_type:
            _name = 'Tags'
            _ = ''

        else:
            _name = 'Posts & Comments'
            _ = 's'

        self.file = {}

        # Testing for single files
        string_like = isinstance(file, str)

        # User passed in multiple files using string deliminator. Check if they exist and add to file test variable
        if isinstance(file, list):
            se_files = {}
            for f in file:
                fp = Path(f).absolute()
                if fp.exists():
                    se_files[fp.stem] = fp

        # File is a string and a 7-Zip file
        elif string_like and '.7z' in file:
            file = self._rename_and_extract_7zip(file, _name)
            se_files = {key: Path(file).absolute() for key, file in file.items()}

        # File is a string and an XML file
        elif string_like and '.xml' in file:
                if '&' not in _name:
                    se_files = {_name: Path(file).absolute()}
                else:  # Single file passed in, but user requested parsing both comments and posts
                    fp = Path(file).absolute()
                    name = fp.stem[fp.stem.find('_')+1:]
                    other = dict(Posts="Comments", Comments="Posts")
                    se_files = {name: fp}
                    self.type = name
                    file, _name = self._find_other_file(fp, other[name])
                    if file and _name:
                        se_files[_name] = file
                    else:
                        raise ValueError("Unable to find matching {} file. Dont use {} content_type, or pass in 7zip \
                                          file, or pass in both Posts and Comments files by delimiting with a ','"
                                         .format(self.content_type, _name))

        # No file was passed in so use the community parameter to fetch the file from the archive
        elif file is None:
            # Make sure community parameter exists and is a viable StackExchange Community
            self.community = self._verify_community_names(community)

            cache = self._check_for_cached(self.community, _name)
            if 'xml' in cache:
                log('STREAM: Cached xml files found!')
                se_files = {file.stem.split('_')[1]: file for file in cache['xml']}

            elif '7z' in cache:
                log('STREAM: Cached 7zip files found!')
                file = cache['7z']
                log('STREAM: {} downloaded. Attempting to decompress {} file{}'.format(file, _name, _))
                se_files = self._rename_and_extract_7zip(file, _name)

            else:
                log('STREAM: No cached files found in project directory')
                # Download the community archive file
                log('STREAM: Attempting to download {}'.format(self.community))
                download_file = self._download_community(self.community)
                # Rename the file's so they have the community tag prepended and extract
                log('STREAM: {} downloaded. Attempting to decompress {} file{}'.format(download_file, _name, _))
                se_files = self._rename_and_extract_7zip(download_file, _name)

        else:
            raise ValueError("File not understood. Please check file parameter and try again.")

        # If user provides an xml file, but doesn't supply the community name, try to determine the community from
        # either the path or the name of the file. If no community is identified, set community to 'unknown'
        if community is None and file:
            coms = {}
            for key, se_file in se_files.items():
                test_com = se_file.parent.parts[-1]
                test_name = se_file.name.split('_')
                if '.com' in test_com:
                    coms[key] = test_com
                elif len(test_name) > 1:
                    if 'stackoverflow' not in test_name[0]:
                        coms[key] = test_name[0] + '.stackexchange.com'
                    else:
                        coms[key] = test_name[0] + '.com'
                else:
                    coms[key] = 'unknown'
            if len(set(coms.values())) == 1:
                self.community = self._verify_community_names(set(coms.values()).pop())
            else:
                raise ValueError("Only one community can be parsed at a time")

        # ensure the file exists and is now in xml format
        for key, se_file in se_files.items():
            assert (se_file.exists()), "Cannot find {}".format(key)
            assert (se_file.suffix == '.xml'), "File {} does not end in '.xml'.".format(key)
            log('STREAM: {} file found'.format(se_file.as_posix()))
            self.file[key] = se_file

        # Lazily load the xml file, puts a blocking lock on the file
        # Just parse posts
        if self.content_type in self._TYPES[:3]:
            self.tree = ET.iterparse(self.file['Posts'].as_posix(), events=['end'])
            self.type = 'Posts'
            self.second_tree = None

        # Parse posts and Comments
        elif self.content_type in self._TYPES[3:6]:

            # Parse posts with Comments
            if self.content_type == self._TYPES[3]:
                self.tree = ET.iterparse(self.file['Posts'].as_posix(), events=['end'])
                self.type = 'Posts'
                self.second_tree = ET.parse(self.file['Comments'].as_posix()).getroot()
                self.second_type = 'Comments'

            # Parse Comments with parent post metadata
            else:
                self.tree = ET.iterparse(self.file['Comments'].as_posix(), events=['end'])
                self.type = 'Comments'

                try:
                    self.second_tree = ET.parse(self.file['Posts'].as_posix()).getroot()
                    self.second_type = 'Posts'

                # a 'Posts.xml' file was not passed in, check for it in the same dir before setting second tree to None
                except KeyError:
                    tree, self.second_type = self._find_other_file(self.file['Comments'], 'Posts')
                    self.second_tree = ET.parse(tree.as_posix()).getroot()

        # Parse tags
        elif self.content_type == self._TYPES[6]:
            self.tree = ET.iterparse(self.file['Tags'].as_posix(), events=['end'])
            self.type = 'Tags'
            self.second_tree = None
            self.second_type = None
        
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
        self.parent_post_attribs = {}

    def _check_for_cached(self, com, file_type):
        files = {x.name: x for x in self.proj_dir.iterdir() if x.is_file()}
        z_name = com+'.7z'
        if '&' in file_type:
            types = [com.split('.')[0]+'_{}.xml'.format(t) for t in file_type.split(' & ')]
        else:
            types = [com.split('.')[0]+'_{}.xml'.format(file_type)]

        if all(t in files for t in types):
            return {'xml': [files[name] for name in types]}
        elif z_name in files:
            return {'7z': files[z_name]}
        else:
            return {}

    def _find_other_file(self, file, other):
        com_file = file.name
        # We only want to load the other file if we know it's from the same community,
        # because the file type is prepended by the community i.e. 'ai_Posts.xml'
        test = file.with_name(com_file.replace("_" + self.type, "_" + other))
        if test.exists():
            return test, other
        else:
            return None, None

    def _get_community_names(self):
        URL = self.URL
        page = requests.get(URL)
        page_html = BeautifulSoup(page.text, 'lxml')
        div = page_html.find('div', class_='download-directory-listing')
        coms, dates = [], set()
        for row in div.select('table.directory-listing-table tr'):
            contents = row.contents
            com = str(contents[1].contents[0].contents[0])
            if '7z' in com:
                coms.append(com.replace('.7z', ''))
                d = contents[3].contents[0][:11]
                dates.add(d[3:].replace('-', ''))

        return coms, dates.pop()

    def _verify_community_names(self, com):
        com = '.'.join(re.split('.|_', com))
        assert isinstance(com, str), "Community name must be in string format. Instead got {}".format(type(com))
        if com not in self.communities:
            log("STREAM: {com} not found in online archive at {url}".format(com=com, url=self.URL))
            raise ValueError("StackExchange community--{com}--not found in online archive at {url}"
                             .format(com=com, url=self.URL))
        else:
            return com

    def _rename_and_extract_7zip(self, file, name):
        program = find_program()
        if program is None:
            raise EnvironmentError("7-Zip not found in OS environment. Archive cannot be extracted")

        # Path might have a partial or full path, convert to Path object, check if it exists, then pass just the name
        # to

        se_file_name = Path(file).absolute()
        assert (se_file_name.exists()), "Cannot find {} file. Please check the path name and try again"\
            .format(se_file_name.as_posix())
        file_name = se_file_name.name
        file_path = se_file_name.as_posix()

        posts = 'Posts' in name
        comments = 'Comments' in name
        tags = 'Tags' in name

        com_name = file_name.split('.')[0]
        if (posts and not comments) or (not posts and not comments and not tags):

            input_names = ['Posts.xml']
            output_names = ['{}_Posts.xml'.format(com_name)]
        elif comments and not posts:
            input_names = ['Comments.xml']
            output_names = ['{}_Comments.xml'.format(com_name)]
        elif comments and posts:
            input_names = ['Posts.xml', 'Comments.xml']
            output_names = ['{}_Posts.xml'.format(com_name), '{}_Comments.xml'.format(com_name)]
        elif tags:
            input_names = ['Tags.xml']
            output_names = ['{}_Tags.xml'.format(com_name)]

        else:
            self.type = None
            return None
        parent = se_file_name.parent

        archive_details = capture_7zip_stdout([program, "l", "-ba", "-slt", file_path])
        output_files = {}
        for input_name, output_name in zip(input_names, output_names):
            out_path = parent.joinpath(output_name)
            if archive_details.get(input_name, None):
                subprocess.call([program, "rn", "-ba", file_path, input_name, output_name])
            subprocess.call([program, "e", "-ba", file_path, "-o{}".format(parent), output_name, "-aoa"])
            output_files[input_name.replace('.xml', '')] = out_path
        return output_files

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
        local_filename = self.proj_dir.joinpath(url.split('/')[-1])

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
        time.sleep(2)  # Under conditions of heavy disk usage, the filesystem may not unlock the file for a few seconds
        return local_filename

    def _clean_text(self, text):
        stripper = self._TagStripper()
        stripper.feed(text)
        text = stripper.get_data()

        # Remove extra newlines or all newlines
        if self.newlines:
            cleantext = self.newline.sub(r'\n', text)
        else:
            cleantext = self.newline.sub(r' ', text)
        return cleantext

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

    def __next__(self):
        return self.iter.__next__()

    def __iter__(self):
        # Get values for filtering out rows
        if self.resume_from is not None or self.resume_from is not False:
            key, value = self.resume_from.items()
        else:
            key, value = None, None

        # Iterate through the file and yield the text
        for _, child in self.tree:

            self.total += 1
            log("STREAM: Fetching {} child element".format(self.total))

            # Start of file, check that the file matches the expected content_type
            if child.tag != 'row':
                if self.content_type in self._TYPES[:4]:
                    assert(child.tag == 'posts'), "Input file is not a StackExchange Posts.xml file. \
                    Please check the path name and try again"

                elif self.content_type in self._TYPES[4:6]:
                    assert(child.tag == 'comments'), "Input file is not a StackExchange Comments.xml file. \
                    Please check the path name and try again"

            else:   
                atb = child.attrib
                # If the user wants to resume parsing from a previous stopping point
                # then check for the value of that attribute for the current child
                if key is None or value is None:
                    pass  # Not filtering results
                elif key == 'Id':
                    item = atb.get(key, None)
                    if value != item:
                        child.clear()
                        continue
                elif key == 'Date':
                    default = '2001-01-01'
                    create = atb.get('CreationDate', default)
                    edit = atb.get('LastEditDate', default)
                    active = atb.get('LastActivityDate', default)

                    item = max(create, edit, active)
                    if value > item:
                        child.clear()
                        continue

                # Fetch the necessary information based on the content_type specified
                if self.content_type in self._TYPES[:4]:
                    # Assemble the prodigy stream compliant dictionary object
                    info = {"meta": {"source": "StackExchange", "Community": self.community, "file_type": self.type}}

                    id = atb.get('Id', None)
                    title = atb.get('Title', None)
                    body = atb.get('Body', None)
                    tags = atb.get('Tags', None)
                    if tags is not None:
                        tags = self._parse_tags(tags)
                    posttype = int(atb.get('PostTypeId', None))
                    answers = int(atb.get('AnswerCount', 0))
                    comments = int(atb.get('CommentCount', 0))

                    if self.content_type == 'all_text' and comments > 0:
                        post_comments = self.second_tree.findall("*[@PostId='{}']".format(id))
                        if post_comments:
                            comments_text = [comment.attrib['Text'] for comment in post_comments]
                        else:
                            comments_text = []
                    else:
                        post_comments = None
                        comments_text = []

                    # Preserve Tag information from Questions for reference by Answers
                    if posttype == 1:
                        if answers > 0:
                            self.parent_post_attribs[id] = {'tags': tags, 'title': title, 'answers': answers, 'seen': 0}
                        parentid = None

                    # If this post is an answer, lookup the tags and title of the parent question
                    elif posttype == 2:
                        parentid = atb.get("ParentId", None)
                        parent = parentid in self.parent_post_attribs.keys()
                        
                        if parent:
                            # Update the seen answer count
                            self.parent_post_attribs[parentid]['seen'] += 1
                            # Get parent attributes
                            tags = self.parent_post_attribs[parentid].get('tags', None)
                            title = self.parent_post_attribs[parentid].get('title', None)

                            if self.parent_post_attribs[parentid]['seen'] >= \
                                    self.parent_post_attribs[parentid]['answers']:
                                # We've seen all the answers, delete the Parent Id entry to free up memory
                                del self.parent_post_attribs[parentid]
                        else:
                            tags = None
                            title = None
                    else:  # Only interested in Post Type 1 (Question) and 2 (Answer)
                        child.clear()
                        continue

                    # If the user only wants text from Posts with specific stackExchange tags,
                    # only return content that matches. Naively iterates through the stream of Posts.
                    # It does not know if the tag actually exists.
                    # If the user supplies tags, and no tags match, skip this post
                    if self.onlytags and not tags:
                        child.clear()
                        continue

                    elif self.onlytags and not any([tag for tag in tags if tag in self.onlytags]):
                        child.clear()
                        continue
                    # This post has what we want
                    else:   
                        
                        if self.content_type == 'all_text':
                            if post_comments:
                                text = title + '\n' + body + '\n'.join(comments_text)
                            else:
                                if title and body:
                                    text = title + '\n' + body
                                elif body and not title:
                                    text = body
                                elif title and not body:
                                    text = title
                                else:
                                    text = None

                        elif self.content_type in 'post_both':
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

                    # unescape HTML encoding and remove html tags
                    # Preserve the original HTML
                    info['html'] = text

                    # TODO: Sometimes causes problems in the HTML stripper, disable for now, investigate later
                    # text = html.unescape(text)
                    cleantext = self._clean_text(text)

                    # Append the text and additional metadata to the stream dictionary
                    info['text'] = cleantext
                    info['meta']['Id'] = int(id)
                    if posttype == 2:
                        info['meta']['ParentTitle'] = title
                        info['meta']['ParentTags'] = tags
                        info['meta']['ParentId'] = int(parentid)
                    else:
                        info['meta']['Title'] = title
                        info['meta']['Tags'] = tags
                    info['meta']['FavoriteCount'] = int(atb.get('FavoriteCount', 0))
                    info['meta']['PostScore'] = int(atb.get('Score', 0))
                    info['meta']['CommentCount'] = comments
                    info['meta']['Views'] = int(atb.get('ViewCount', 0))
                    aa = atb.get('AcceptedAnswerId', None)
                    info['meta']['AcceptedAnswer'] = int(aa) if aa is not None else aa
                    info['meta']['CreationDate'] = atb.get('CreationDate', None)
                    info['meta']['LastEditDate'] = atb.get('LastEditDate', None)
                    info['meta']['LastActivityDate'] = atb.get('LastActivityDate', None)

                    # yield the dictionary
                    self.parsed += 1
                    if _ % 10000 == 0:
                        log("STREAM: {p} of {t} XML child element parsed".format(p=self.parsed, t=self.total), info)
                    yield info

                elif self.content_type in self._TYPES[4:6]:
                    # Assemble the prodigy stream compliant dictionary object
                    info = {"meta": {"source": "StackExchange", "Community": self.community, "file_type": self.type}}
                    id = atb.get('Id', None)
                    postid = atb.get('PostId', None)
                    body = atb.get('Text', None)
                    # Get attributes from parent post
                    if self.second_tree:
                        parent = self.second_tree.find("*[@Id='{}']".format(postid))
                        parent_tags = parent.attrib.get('Tags', '')
                        if parent_tags:
                            parent_tags = self._parse_tags(parent_tags)
                        parent_title = parent.attrib.get('Title', '')
                        parent.clear()
                    else:
                        parent_tags = None
                        parent_title = None

                    if self.onlytags and not parent_tags:
                        child.clear()
                        continue

                    elif self.onlytags and not any([tag for tag in parent_tags if tag in self.onlytags]):
                        child.clear()
                        continue

                    # This comment has what we want
                    else:
                        if self.content_type == 'comments_both' and parent_title:
                            text = parent_title + '\n' + body
                        else:
                            text = body

                        # unescape HTML encoding and remove html tags
                        # Preserve the original HTML
                        info['html'] = text

                        # Check to see if valid text was found, if not, skip to the next xml child element
                        if text is None:
                            child.clear()
                            continue

                        else:
                            cleantext = self._clean_text(text)

                            # Append the text and additional metadata to the stream dictionary
                            info['text'] = cleantext
                            info['meta']['Id'] = int(id)
                            info['meta']['PostId'] = int(postid)
                            info['meta']['Score'] = int(atb.get('Score', 0))
                            info['meta']['CreationDate'] = atb.get('CreationDate', None)
                            info['meta']['PostTitle'] = parent_title
                            info['meta']['PostTags'] = parent_tags

                            # yield the dictionary
                            self.parsed += 1
                            if _ % 10000 == 0:
                                log("STREAM: {p} of {t} XML child element parsed".format(p=self.parsed, t=self.total), info)
                            yield info

                else:
                    break

            # clear the child from memory before moving to the next child element
            child.clear()

  
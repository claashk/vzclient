import re

multiline_comment = re.compile(r"/\*.*?\*/", re.MULTILINE|re.DOTALL)
trailing_comment = re.compile(r"([\{\},\w])\s*//[^\"\n]*\n", re.MULTILINE)
comment_line = re.compile("^\s*//.*\n", re.MULTILINE) 

with open(path) as ifile:
    commented_text = ifile.read()

clean_json = trailing_comment.sub(
                 r"\1\n",
                 comment_line.sub(
                     "",
                     multiline_comment.sub("", commented_text)))

cfg = json.loads(clean_json)

for meter in cfg['meters']:
    for ch in meter['channels']:
        # do something with channel. has properties uuid, api, middleware, identifier
        
# cfg['mqtt'] contains host, port, topic, enabled.
# channel names are: topic.replace("/", ".") + "chn{}".format(channel_number) + "raw" / "agg"

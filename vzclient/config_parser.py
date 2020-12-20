import re
import json


multiline_comment = re.compile(r"/\*.*?\*/", re.MULTILINE|re.DOTALL)
trailing_comment = re.compile(r"([\{\},\w]|\".*\")\s*//[^\n]*\n", re.MULTILINE)
comment_line = re.compile("^\s*//.*\n", re.MULTILINE)


def remove_json_comments(string):
    txt = trailing_comment.sub(r"\1\n",
                               comment_line.sub("",
                                                multiline_comment.sub("",
                                                                      string)))
    return "\n".join([line for line in txt.split("\n") if line.strip()])

def read_vzlogger_config(path):
    """Parse vzlogger.conf

    Arguments:
        path (str or pathlib.Path): Path to config file

    Return:
        dict: Dictionary with config options
    """
    with open(path) as ifile:
        return json.loads(remove_json_comments(ifile.read()))
    return

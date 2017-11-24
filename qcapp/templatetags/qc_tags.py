from django import template
import re

register = template.Library()

@register.filter
def max_item_count(dict_list, key):
    max_item = 0
    for dict in dict_list:
        length = len(dict[key])
        if length > max_item:
            max_item = length
    return max_item


@register.filter
def clean_special(value):
    # Remove special characters
    return re.sub('[^a-zA-Z0-9]', '', value)

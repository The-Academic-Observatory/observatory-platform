#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#


from typing import List, Tuple

from bs4 import BeautifulSoup


def strip(string: str) -> str:
    return string.strip(' \n\t\r')


class HtmlParser:

    def __init__(self, page, parser='lxml'):
        self.page = page
        self.parser = parser
        self.soup = BeautifulSoup(page, self.parser)

    def get_raw_content(self) -> str:
        return self.soup.decode()

    def get_title(self) -> str:
        title_element = self.soup.find('title')
        try:
            title = strip(title_element.text)
        except AttributeError:
            title = ""
        return title

    def get_links(self) -> List[Tuple[str, str]]:
        links = []
        a_elements = self.soup.find_all('a')
        for a in a_elements:
            url = a.get('href')
            text = strip(a.get_text())

            # Only add a link if it has a URL. Still add if no text.
            if url is not None:
                links.append((url, text))

        return links

    def get_full_text(self) -> str:
        soup = BeautifulSoup(self.page, self.parser)

        # Remove all style and script elements
        for name in ["style", "script"]:
            elements = soup.find_all(name)
            for element in elements:
                element.decompose()

        # Get text
        text = soup.get_text(strip=True, separator='\n')
        return text

# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Cameron Neylon

import seaborn as sns

# Variable Sets - Column selectors for data frames
output_types = [
    'Journal Articles',
    'Proceedings',
    'Books',
    'Book Sections',
    'Edited Volumes',
    'Reports‡',
    'Datasets‡'
]

oa_types = [
    'Open Access (%)',
    'Total Gold OA (%)',
    'Total Green OA (%)',
    'Hybrid OA (%)',
    # 'Green in IR (%)',
]

# Palettes -

region_palette = {
    'Asia': 'orange',
    'Europe': 'limegreen',
    'North America': 'dodgerblue',
    'Latin America': 'brown',
    'Americas': 'dodgerblue',
    'Africa': 'magenta',
    'Oceania': 'red'
}

oatypes_palette = {
    'Open Access (%)': 'black',
    'Total Gold OA (%)': 'gold',
    'Total Green OA (%)': 'darkgreen',
    'Hybrid OA (%)': 'orange',
    'Bronze (%)': 'brown',
    'Green in IR (%)': 'limegreen'
}

husl = sns.color_palette(n_colors=len(output_types))
outputs_palette = dict([(output_type, husl[i])
                        for i, output_type in enumerate(output_types)])
outputs_palette.update({'Total Outputs': 'black'})

# Standard Name Changes
country_clean = {"country": {
    "United Kingdom of Great Britain and Northern Ireland":
    "United Kingdom",
        "Iran (Islamic Republic of)": "Iran",
        "Korea, Republic of": "South Korea",
        "Taiwan, Province of China": "Taiwan"
}
}

outputs_clean = {'type': {
    'total': 'Total Outputs',
    'journal_articles': 'Journal Articles',
    'proceedings_articles': 'Proceedings',
    'authored_books': 'Books',
    'book_sections': 'Book Sections',
                     'edited_volumes': 'Edited Volumes',
                     'reports': 'Reports‡',
                     'datasets': 'Datasets‡'
}}

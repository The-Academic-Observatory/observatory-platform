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
    #'Green in IR (%)',
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

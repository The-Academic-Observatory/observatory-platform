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

# Author: Richard Hosking

import json
import xml.etree.ElementTree as ET

def new_funder_template():
    """ Helper Function for creating a new Funder.
    :return: a blank funder object.
    """
    return {
        'funder': None,
        'pre_label': None,
        'alt_label': [],
        'narrower': [],
        'broader': [],
        'modified': None,
        'created': None,
        'funding_body_type': None,
        'funding_body_sub_type': None,
        'region': None,
        'country': None,
        'country_code': None,
        'state': None,
        'tax_id': None,
        'continuation_of': [],
        'renamed_as': [],
        'replaces': [],
        'affil_with': [],
        'merged_with': [],
        'incorporated_into': [],
        'is_replaced_by': [],
        'incorporates': [],
        'split_into': [],
        'status': None,
        'merger_of': [],
        'split_from': None,
        'formly_known_as': None,
        'notation': None
    }

def parse_fundref_registry_rdf(file_name):
    """ Helper function to parse a fundref registry rdf file and to return a python list containing each funder.
    :param file_name: the filename of the registry.rdf file to be parsed.
    :return: A python list containing all the funders parsed from the input rdf
    """
    funders = []
    funders_by_key = {}

    tree = ET.parse(file_name)
    root = tree.getroot()

    for record in root:
        if(record.tag == "{http://www.w3.org/2004/02/skos/core#}Concept"):
            funder = new_funder_template()
            funder['funder'] = record.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
            for nested in record:

                if(nested.tag == '{http://www.w3.org/2004/02/skos/core#}inScheme'):
                    continue

                elif(nested.tag == '{http://www.w3.org/2008/05/skos-xl#}prefLabel'):
                    funder['pre_label'] = nested[0][0].text

                elif(nested.tag == '{http://www.w3.org/2008/05/skos-xl#}altLabel'):
                    funder['alt_label'].append(nested[0][0].text)

                elif(nested.tag == '{http://www.w3.org/2004/02/skos/core#}narrower'):
                    funder['narrower'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://www.w3.org/2004/02/skos/core#}broader'):
                    funder['broader'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://purl.org/dc/terms/}modified'):
                    funder['modified'] =  nested.text

                elif(nested.tag == '{http://purl.org/dc/terms/}created'):
                    funder['created'] =  nested.text

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}fundingBodySubType'):
                    funder['funding_body_type'] =  nested.text

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}fundingBodyType'):
                    funder['funding_body_sub_type'] =  nested.text

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}region'):
                    funder['region'] = nested.text

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}country'):
                    funder['country'] = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}state'):
                    funder['state'] = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']

                elif(nested.tag == '{http://schema.org/}address'):
                    funder['country_code'] = nested[0][0].text

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}taxId'):
                    funder['tax_id'] = nested.text

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}continuationOf'):
                    funder['continuation_of'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}renamedAs'):
                    funder['renamed_as'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://purl.org/dc/terms/}replaces'):
                    funder['replaces'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}affilWith'):
                    funder['affil_with'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}mergedWith'):
                    funder['merged_with'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}incorporatedInto'):
                    funder['incorporated_into'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://purl.org/dc/terms/}isReplacedBy'):
                    funder['is_replaced_by'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}incorporates'):
                    funder['incorporates'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}splitInto'):
                    funder['split_into'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://data.crossref.org/fundingdata/terms}status'):
                    funder['status'] = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}mergerOf'):
                    funder['merger_of'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}splitFrom'):
                    funder['split_from'] = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']

                elif(nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}formerlyKnownAs'):
                    funder['formly_known_as'] = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']

                elif(nested.tag == '{http://www.w3.org/2004/02/skos/core#}notation'):
                    funder['notation'] = nested.text

                else:
                    print(nested)

            funders.append(funder)
            funders_by_key[funder['funder']] = funder
    
    for funder in funders:
        children, returned_depth = recursive_funders(funders_by_key, funder, 0, 'narrower')
        funder["children"] = children
        funder['bottom'] = len(children)>0

        parent, returned_depth = recursive_funders(funders_by_key, funder, 0, 'broader')
        funder["parents"] = parent
        funder['top'] = len(parent)>0
    
    return funders

def recursive_funders(funders_by_key, funder, depth, direction):
    starting_depth = depth
    
    children = []
    
    for funder_id in funder[direction]:
        sub_funder = funders_by_key[funder_id]
        name = sub_funder['pre_label']

        returned, returned_depth = recursive_funders(funders_by_key, sub_funder, starting_depth+1, direction)
        
        if(direction == "narrower"):
            child = {'funder': funder_id, 'name': name, 'children': returned}
        else:
            child = {'funder': funder_id, 'name': name, 'parent': returned}
        children.append(child)
        
        if returned_depth > depth:
            depth = returned_depth       
    return children, depth


import os
import unittest
import urllib.request
from zipfile import ZipFile

from academic_observatory.utils import HtmlParser, unique_id, get_home_dir, get_file

EXPECTED_TITLES = [
    'Curtin University Sustainability Policy Institute » Blog Archive  » Curtin’s own Pioneering Australian',
    'Curtin University of Technology', 'OASIS - Student Portal',
    'Collaborative centres, our research - Science & Engineering | Curtin University, Perth, Western Australia',
    'Entrepreneurship degree: Curtin University',
    'Making the decision, coursework degree, postgraduate - Future Students | Curtin University, Perth, Australia',
    "'Victor Hugo Concerning Shakespeare and Everything' – No by-line (but likely Curtin), Westralian Worker, 20 December 1918",
    'Curtin University Library', 'C-MARC Staff', 'Geodesy', '',
    'Regional Western Australian students - Library - Curtin University',
    'Current Students - Do not Forget - Curtin University of Technology',
    'From Poor Boy to Prime Minister - Curtin family 1942',
    'Borrowing from another WA university - Library - Curtin University', 'DigiTool Collections',
    'Humanities - Study & research tools - LibGuides at Curtin University',
    'Credit for previous study, credit transfer, postgraduate degree - Future Students | Curtin University, Perth, Australia',
    'Master of Chemical Engineering: Curtin University', 'Master of Occupational Health and Safety: Curtin University',
    'Open Universities Australia students - Library - Curtin University', 'Sexology: Curtin University',
    'Products, CMST, marine science -\xa0Centre for Marine Science and Technology | Curtin University, Perth, Australia',
    'space Archives - News and Events', 'WAIN Info Page',
    'Conducting research, academic staff, postgraduate students - Research | Curtin University, Perth, Australia',
    'Curtin University of Technology',
    'Contact us - Curtin Business School | Curtin University, Perth, Western Australia', '301 Moved Permanently',
    'Home, structural dynamics, CIMP -\xa0Centre for Infrastructural Monitoring and Protection | Curtin University, Perth, Australia',
    'Courses at Curtin', 'Our Projects', 'Research - Health Sciences | Curtin University, Perth, Western Australia',
    'New navigation gadget for people who are blind - News and Events | Curtin University, Perth, Western Australia',
    'Creativity: Research and Practice in Human Resource Management',
    'Centre for Human Rights Education » Presentations', 'Graduate Diploma in Project Management: Curtin University',
    'Humanities Online Teaching and Learning', 'Library blog » Blog Archive  » 129 years ago on this day…',
    'Faith services and facilities',
    'Physics and Astronomy - Science & Engineering | Curtin University, Perth, Western Australia',
    'Curtin researchers unlock the secrets of acupuncture - News and Events | Curtin University, Perth, Western Australia',
    'Nature ranks Curtin in the top 10 in Australia - News and Events | Curtin University, Perth, Western Australia',
    'Research and Practice in Human Resource Management',
    'Online handbook 2004 - Post Harvest Horticulture and Quality Management 301',
    'Articles by Issue : Research and Practice in Human Resource Management',
    'National Drug Research Institute - Seminars and Conferences',
    'Our people, electrical and computer engineering - Science & Engineering | Curtin University, Perth, Western Australia',
    'Curtin University Sitemap', '301 Moved Permanently', 'Graduate Certificate in Taxation: Curtin University', '', '',
    'Home - Curtin Humanities',
    'Curtin recognised as one of the world’s most international universities - News and Events | Curtin University, Perth, Western Australia',
    'Future Students - Study At University - Curtin University Sydney',
    'Sitelines - Humanities | Curtin University, Perth, Western Australia', 'Home', 'View staff profile',
    'Curtin Challenge', 'Petroleum Engineering - Science & Engineering | Curtin University, Perth, Western Australia',
    'Soldier for a crisis: Field Marshal Sir Thomas Blamey', 'The Geoff Gallop Collection',
    'Courses and programs - Curtin Business School | Curtin University, Perth, Western Australia',
    'Master of Science (Dryland Agricultural Systems): Curtin University', 'Curtin news - Sport and lifestyle',
    'Research - Eunjeong Jeon', '301 Moved Permanently',
    'Chemical Engineering - Science & Engineering | Curtin University, Perth, Western Australia',
    'Master of Information Systems and Technology: Curtin University',
    'Centre for Human Rights Education » Blog Archive  » Opinion Piece: What happens in Curtin, stays in Curtin',
    'Margaret River Education campus, Centre for Wine Excellence, environment and agriculture - Science & Engineering | Curtin University, Perth, Australia',
    'View Scholarship', 'Corporate Information Management degree: Curtin University',
    'Computer Science: Curtin University',
    'Services, support and help for sponsored students | Curtin University, Perth, Australia',
    'Legislative Changes - Fees @ Curtin - Curtin University',
    'Architecture and construction - Study Areas - Curtin University', 'Public Health: Curtin University',
    'Obesity in childhood leads to poor posture and back pain - News and Events | Curtin University, Perth, Western Australia',
    'Home, energy science, fuel conversions - Fuels and Energy Technology Institute | Curtin University, Perth, Australia',
    'Primary Education degree: Curtin University', 'Fees and dates', 'A Brief Biography of Douglas MacArthur',
    'Graduate Research School, higher degree, postgraduate - Future Students | Curtin University, Perth, Australia',
    'Food Science and Technology - Health Sciences | Curtin University, Perth, Western Australia',
    "On Track: Curtin's Railway Journeys", 'Object moved',
    'Expression of interest, Graduate Research School, higher degree - Future Students | Curtin University, Perth, Australia',
    'Student showcase - Humanities | Curtin University, Perth, Western Australia', 'View Scholarship',
    'Science and Engineering - Home', 'WA university students & staff - Library - Curtin University',
    'Crisis at Home and Abroad -  American troops, Melbourne, 1942', '',
    'Strategic Flashlight Blog » international security',
    'Master of Engineering Science (Petroleum Engineering): Curtin University',
    'Web Media (Mass Communication) degree: Curtin University', 'Research courses - Curtin University']

EXPECTED_LINK_URL_HASHES = ['f5e2a90bc869ee55d40cbfe45553f913', 'ce2cb5bedf5228fd3e913fadde919b78',
                            '18cbd10b40887e5075a88a69a21485c4', '0c4d4cb5bf807b00af273e972a45771d',
                            '2168b6069e9613b673bff5fa838b7932', '099a289ba514f98f2d97ed4b96258f58',
                            'ad3bc4ea3b1b5b06dea4d7ef37ecf021', '1b256bc6cdacd3a4197d01c890198571',
                            'f22e8fccd6f766095ef70cc66f47b907', 'f9babe1aeee695731897e4e94c100a77',
                            'd41d8cd98f00b204e9800998ecf8427e', 'a50128b959b7474d8970ddb7fb8e5df9',
                            '180003d92bda2349b9be57aca2310e35', 'a39198b8558b8274cf418064ba62648e',
                            'ca7e31a254cac0b4f774948546272171', '5c122363844342adaa70b63fe959bdc8',
                            '1621b47a1abc5c0a3e51fa20056b7cf9', '1ac0676eb1564b09c435618d6608cb03',
                            '87a5a649662be4393ac96f34b2b88d7c', '7921862d4c3e8004dfae61da6a0f4518',
                            '9ba1bd47a9070cdbc520314abf2f5d29', '519c6f447a2c00d47d8245627253c4e8',
                            '8b7535807f4dff1fa49b1c9ae4d6650c', '5339242c63dc87fa178e3ada2af1c670',
                            '4cd3a5787f5ec8f93faa6dc41ee762b8', '4750dd8e58e80c9ad812855ab44d9ca7',
                            'ce2cb5bedf5228fd3e913fadde919b78', 'f91e8f57bdf5fab7c122221fd9caf42a',
                            '8b0853795e6b81e3db47ea853a0e4e91', '9ddb2f4f5c1cd24fe42f24ddde38851b',
                            '5f06b57f457ac69dc7a1487efd66baa3', 'e0f2ed1787c55c9e5e532b0ce108dd2b',
                            '2c8768fc846dfb7e1472d7951a79a47e', '8765b93ff2a82f31914646587276d634',
                            'ce475fd4c247b80eff9bb5175c4eb77a', 'c2c3b2703bb2fbdee9aa70c0baa43306',
                            '69f936a4d6ad56b4b7a997fb3293a9b5', '0ceb8e1b24b708cf0f474be50f10a6bd',
                            '72c8c7a73fab30f812855867f13c1752', 'c19f5f62da16fef4b8069214a5e8ddb1',
                            '45f322a929edffb4f78d6d7c58e0c81d', 'dab85a683e7823601e9cd0ea366883dd',
                            'fc6b13257c6fa9c9cc01574660fb04f3', 'd41d8cd98f00b204e9800998ecf8427e',
                            '7b2461e8192eb82441e15fcdb130fb77', 'fcd8c9ac8ce5ffb0e0f265788b6b1215',
                            '5b1b1a1f51952ba4c913dee0b0986e0b', '9a19d32bcf3f513f60971a2edbc7313e',
                            'b017c7175123f72a1be7d92e47102391', '0d72729579864e990fd82f698a39a39a',
                            '035440d36b9db6e0b30f46abce5b6ed6', 'd41d8cd98f00b204e9800998ecf8427e',
                            'd41d8cd98f00b204e9800998ecf8427e', '124dcf3ebc80c85097d89f7fe0eac747',
                            'b4faa4af85e8b692e610b2a410727289', '694c75834e3debf8dd1c3106fc462f3c',
                            'f2925ff25c79fd87ec25a46077ee81d0', '4516fa341ce6a8dbe88ec05ab242aa38',
                            '477f8b5b35bc38bfdfaf994c8ad9337c', 'acebf291e74228117f2027b24bcdf464',
                            'e534e18bbc08c6599d0285aeac17d42b', 'ae0f0feb89a3a8218ccb3a511f1cae56',
                            'da00da63f0b17063d58c214b95df3013', 'b15160efd4148fbffe93e5bc3bd0696a',
                            '2dfc605b4a39b19dc96e30a84c02ad32', '63b0c2b9584c6a049cb2c1bbbfecb6c1',
                            '2b81f280d3446207fa945faf1ff76333', '77a23fa4be1609494555bb46cb806284',
                            'd18ada06a8c315b09cb59513d1c72898', 'f26a58ab4d69ca36d752c882d08dce67',
                            'cb02b89ca2049762b4575e9981549eda', '226a669cf50d4adb2e389786490366af',
                            '35ca391e9192a55a29a0718a4872e0d5', 'e9ace5e7582abca1e9de21b5f1305a7c',
                            'f1bac2f40de5e23df9d51a9f086e78ff', 'f196b0e16c7dc8928cf1bb103e439f99',
                            '29386bc394c049617188d8109ab9013a', 'c85a7881face4b31770c08fc966b72b5',
                            'f181523ef613195a09de96d0a675e92a', 'f135d7ce7d27fb555ac0d8b4ee1e1239',
                            '20896d4b3c751f160e502aacd5022bbb', 'e09c222c86c92e0e023d6b89c98d49e8',
                            '621fc731930ea11671e2132d0c29f68d', 'feb448ecf432f0c770a2aaf2213003f8',
                            '7d9506cb708595cc3eb8c3961864ab1a', '555eeb7b75c065699c3f01d68a0a29d0',
                            'd2c0ccaea4527c4088997ccc3bab5455', 'cd75e3c12998ac0f1b14ab7e72bdd47a',
                            '09cb14ff334584d2a1ec93f8248304aa', '09bbefd09226a6ae4b61280b7218ced2',
                            '1db395a195b2ebb37a4ebd4cc0a071a0', '56c1c32bc032443aef2c558b55fc47b7',
                            '11df0ed4e1a947af465ee79d637d3e50', '40bac65d0264a7ddc9df7b0e1615fe7c',
                            'd41d8cd98f00b204e9800998ecf8427e', '94bf63ee64a36cb277bffed0086ab30d',
                            'b8b3dac75ec0d5692802d6159298767d', 'bcfc21fec8a27f49720c6b30b29c2741',
                            '7237e7b4c30ace5ee16062d202168d4b']

EXPECTED_LINK_TEXT_HASHES = ['afedc95ec22ec0bf7ae983e80dc51399', '01bdd402976275dcb1c75782f73d857b',
                             'bd83f5579bc6957274d0474652c889ff', '7dcbf247887619c2aff35c0d036c19d0',
                             'd109d3870f63da5fd30f32f4b21f99ae', '1453e12e941fc7f5efd1bb878e7486e5',
                             '6986166bdf4c44ba1075db918072310f', '1d09f0eb4d16563c4263cd065bab9bfa',
                             '2f5a3787a86049c0587db2d58bb1bd24', '3ad9412198e081ee2f1342f3a3409963',
                             'd41d8cd98f00b204e9800998ecf8427e', '3539989b98045ad0e69b1958a19b45d4',
                             '5570da35966cdc7ee7c766ddc5d3711a', 'd41d8cd98f00b204e9800998ecf8427e',
                             '30e69b91070a73e820bfed0d2b928d54', '3ff18ac2cd1cc777825b980ebb66cf9f',
                             '28492ce61e0da60b2af1a361042e7d6c', '32b5d894bf80843ef048b39e49ebf26a',
                             '7570fff6ddcf5f583a4c342a5396e616', '6a5cce74042d293e2b6509e7a8476fef',
                             '2078ebafdf9dd43e69d7c14d317f360a', '3dfbd2086d52cc01b1c69d25919e0722',
                             'ab55a3b74f02324f9047fb1dbde11eef', '7d0ce5b3b00911b2ed0d58ae7e5495e5',
                             'd4ac9cad373cc39696c7e356b6479df3', '0efaf19256b9c6803ad0c5e37199f6bd',
                             '01bdd402976275dcb1c75782f73d857b', 'f6ab0c007bd27fdedad88d00531b7635',
                             '6c92285fa6d3e827b198d120ea3ac674', 'a3652f8a24c15eb137a0cd503f845579',
                             'b10abd2263ad4d44fca8e2f310adb193', '8a7f3d33253c4c92ba754aaeddafd030',
                             'd96dc4974a4a52fba4c39f948b053693', 'bf2ef82c7c15c88ec5a39399b88789bd',
                             'fd5e56ee20df64f56e3b47e5724e3790', '2ac83681cd7c149c5bb9ad649cf9c056',
                             'a30ec9805003e80195cadab5af82c92b', '64b13bdb12a71d7d830540c73d258e40',
                             '893510ba2aff3199f0f88bedb7e8c114', 'd614c477f33256582649fdc2294beeb2',
                             'd5b2baf3751f19985917c117b303c636', '6f1b44ee51d9d353117d9761b1dfb934',
                             '8693f27cf599b19e39e6209549572fff', 'd41d8cd98f00b204e9800998ecf8427e',
                             'df75b4fbe3bf5fc92fb9a8cc52fa57de', 'be4b35b00a0360bb447bf02739294940',
                             '831db69025d51135ca3e7a15150c00f1', 'aca555643e1f52e6d908b10bd8632130',
                             '4127296d1d8401da367dc0ee2c701d09', '6c92285fa6d3e827b198d120ea3ac674',
                             '909269e2d1668dd8d0de6ef588dc42e9', 'd41d8cd98f00b204e9800998ecf8427e',
                             'd41d8cd98f00b204e9800998ecf8427e', '88889c1032e5d94ea916396429bac46f',
                             '6c0b7ba89c397785fe6e493540243de3', 'dd3b6b51ced084418b1799335a918949',
                             'f60b8f77d65f067c39f7b70bb4dcaa39', 'ec9c64c6081b6041f9ca852aa83fcdbd',
                             '1b652fed8c856842681dba21ba000300', '539dccc5664df05244bcd24862ead546',
                             'b11ddf4008aa09f037d181ae562e990c', '7c30147366361059c289a7972d216718',
                             'dd9d7aaed6aaa258ea09ffabad91ca10', '9acf995bc84decb15273715a0137f4b4',
                             '1d482c01063d07846256a42aa57885f0', '949e99182729207b533ade033da5c6a7',
                             '4e0274ce6876f1a22ccff60180da3dc9', '6c92285fa6d3e827b198d120ea3ac674',
                             'ac5b298b2aa34a04b68eaddf817314f1', '8e79e439f78fe44678e1888e2b565d0b',
                             '47a6c6e9b26e836ed29782d628868de7', '4e5f4a835920da9b17ef0ae96f046cd8',
                             '4e028a76b6727b6bb3d60d3d33297b14', 'a981ccfcd17663c1295dfbad0d2f1edb',
                             '08fc150f98eec5535c3f2ec759770ef4', 'eb7953fea7285389011f5cba0a8a8f6d',
                             'df2eef24643c970af185a88f0f73ad6a', 'dda30f23869151b5477f1c3e66051a18',
                             'e51c8d3e756ec62c60eb01a38e592ae4', '642ef4ab3bd4f6818620ac2c613765f8',
                             '841b30c23d33bfe28bc837efa8c61c6a', '741fd054fa4797855ca91ee3348d03fc',
                             '0119c77e5ab71ca92eae748ee33f3ec1', '75960528daa11ba502bed7bcdef1bab0',
                             '1407e58a21f9179cfc82aab33d71c755', '17edc6e9a58b55bee072d8841a0fe143',
                             'd07523437eecf0a40f38d49767e2bc79', '6c92285fa6d3e827b198d120ea3ac674',
                             'a6047629042f5dd0228cd26f3d54c650', 'b252e0e55d8147dc0bc351abde14a162',
                             '4e028a76b6727b6bb3d60d3d33297b14', '5e4a2197a2b0eb9b18e9c6bcc5032aa7',
                             '2323de80fe01a2412a411abd2cb67467', 'd41d8cd98f00b204e9800998ecf8427e',
                             'd41d8cd98f00b204e9800998ecf8427e', '0c3cbfbc656870345a4274521445fbbf',
                             '159fb48507cfa1433889a2006a08d6a1', 'e9e8b81153e4833ce115691e1caebb10',
                             '9b9601c859977cb8cfec4ca557936e38']

EXPECTED_FULL_TEXT_HASHES = ['14bbc281d3934d7b7b6a46d7c8fd0a7a', 'e06e2ae93d6644f08967c273ea4d318f',
                             'a1eb9402a2f38ae531c2fd7b22db574c', '3d3ca7f88154a08dabd8e13f0f3cc1d5',
                             '00e63b3ac4135be79edcc5f7f8a449ee', '3c6918e1d278801f22c262ab0e0a4e51',
                             '33a75d73a429cdc1be2f4b2259efc714', '59ed090e9ba5742e9f9fa9b1e9d907c0',
                             'b230b5c1c5c1216d2d2346674eef80c6', 'cfcea9a38923ba8166f6c2fa099157d7',
                             '96c5637e1eb8f8f8c34172f2d23eafc6', '8fbdbaaac918e544e9c8e4bcfce26f10',
                             '19a39294b391513cd3994ec174e54958', 'c1bd9e4166ba38d12e1e93ea0da4e7e1',
                             '187958f35217f66aa3b172be6c79ae35', '105ffb829763fe196eb4029c385aa782',
                             '76ea674324345c6e34811ac807246b2b', '7d0062c64b579057707b886b0c2eedc0',
                             '10ca0ff4a85981b40ba58317ad923dcd', '63dd1593cad65629fc52fca1a9f0ecff',
                             '0de993493a1123475054469e18e90e25', 'b4f627fe7721a99f37114327de0508f5',
                             '8fa4ff3d1fc1741dcc31bd7f72cf8ed3', '44cda7da2040082334f17b5514a60ce5',
                             'b233350c6dfcc0ffc1a2ed610e568aa9', '6de4c454df57093d7d0705c7c5422f35',
                             'e06e2ae93d6644f08967c273ea4d318f', '7a28905c69020aa3132e14e2a4d712de',
                             'da41af028fc3e88466e13821bd37fc1d', '8420e03908435a150ec8e34ddb41530e',
                             '7cd72648fe43ebcd8ab3a74b970461e2', '87101dd523c3587c6e246e62c04696d3',
                             '626edc2d23ab4264a4dfa16e6588a1d7', '3e75f227b3d8132ea71985cd53c8a52b',
                             '4adf02a5e616d02d6913eaedd7173f06', '0eaa42d7af75c5ea0a77ce427c27cb76',
                             '83ae9a0ce094d4c5f8d608a200843448', 'f8768164c43f73026503d0ec926fc599',
                             '6e698b047d2a6c4e13e74325ed002370', 'c4ce235b73702d0f2f4850d98b6758ff',
                             '521bf3c72c1b9521814e001b971ba750', '609b9cf663a351bc6fca044088bca47c',
                             'e8072b6ba14e4b37b038f5ee3007d123', '7243a0b78a11f64a0b444a98b37c3acd',
                             '9a11011554c6f5676929560071a6755e', 'ffcf3a243578bbcad08081325b1325fd',
                             '475fa696c516c2023c8fb4d5ba64a314', '7d3d14b7545e1b97aa452b287e39dfba',
                             '558f95a4726f5623711cefc226453556', 'da41af028fc3e88466e13821bd37fc1d',
                             '4776d1017cad86814f3c85d90bb2e868', 'd41d8cd98f00b204e9800998ecf8427e',
                             'd41d8cd98f00b204e9800998ecf8427e', '0c1debe1fc0a9ae8d1712d8813ef7d4a',
                             'f1b6cfd22e7854b4b625270a36075c9e', 'c5d804ac163cbab75ab673ec6a8a47d2',
                             'b51aaba41ed2bb32b6b88b41e2a3db24', '735e7eb387b92ef766ed9d410394bbf7',
                             '6bc214487ef98b79cae1b60d7d638429', 'd769bff6c7932088df36d1c2d483ee04',
                             'bfd9ce352a05b3d3b81051a9e760fa78', 'e97dfee25d049ea4bb710a8ae9c5c73b',
                             '2f77e929cdb51bc4b68ca08e2de515f8', '72291b29eac488bb7e65548f2a444567',
                             'a79eda9917e61a833324d0243445e682', 'cb79d51e7ea4f935d4682d1e2d211ba0',
                             '48ab5cb3b730cae374f1a2523ae3effe', 'da41af028fc3e88466e13821bd37fc1d',
                             '1666d1e5cdc661918cb34c45153cccb4', '9d84cba68e33dce22a661b8769bc9a1f',
                             '61ed87842a4f0c20a03e6c9634045117', '27ceeb9c8db040178632a098f1fad790',
                             '9576fd4213f496d7873120d138d52324', 'd5b401df14c4a0d7f0e99c461fa5960f',
                             'bd30727d4a3535710b9e98080605412d', '9eff35dc5dce7da0a8e32da609b82c79',
                             'c2276b4f262603a834b661976429c595', '7a182cf8a6829fde05bbe24de71b25d3',
                             '1dd4309776ed88a4d7f16b0df74cb69c', '2da01c1dcd82df154a60e4008012e41b',
                             '3c68f719eafe05eac55ba81ad708c762', '52e89d33ac6ec5bef60884f3c3990f36',
                             '0e34b2a4cde1a50e34d5c145b2ff4b08', '2c864432fbe9a793094c6785a8d23615',
                             '673b68f58828a700701601e446535389', 'b51a1edc72a84e843637410a68211c55',
                             '0e8191bc8828d44190ed3bb4ba976451', 'e61980c17836258c279bd3dba536be7b',
                             '8fb2af88e3d812cfdf01ad6f48ca5bec', '71ff1abd530eeb62502da490ff908c95',
                             '9576fd4213f496d7873120d138d52324', '10e9daa904d22b1b2ea05ff43f536fb9',
                             '7fbedd4fc3a832633315b2f02b367fef', '580bece0595886a319e5c4ffeade0a63',
                             '515c99044e5a21629cbc1ea11bcd814b', 'b71c598e81deff8a01d387ca140d597a',
                             'e61ba76eb2d32db88b38d4106bc7509e', '2f219b5487fc51c5269ff1b54aac0353',
                             'fdf8e284062938be1d8455b7bcf212a8']


def get_pages(path: str):
    file_names = os.listdir(path)
    pages = []
    for name in file_names:
        if name.endswith(".html"):
            file_path = os.path.join(path, name)
            with open(file_path, 'r', encoding='utf-8') as file:
                data = file.read()
                pages.append(data)
    return pages


def get_tests_dir() -> str:
    cache_dir, cache_subdir, datadir = get_home_dir(cache_subdir="tests")
    return datadir


class TestHtmlParser(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        tests_dir = get_tests_dir()
        test_file_url = 'https://onedrive.live.com/download?cid=6917C8254765425B&resid=6917C8254765425B%21154&authkey=AMSit72vXzUxpqI'
        file_path, download = get_file('webpages.zip', test_file_url, extract=False, cache_subdir="tests",
                                       md5_hash='72af61b056780a95a34feabbf9d9acaf')
        # Unzip
        extract_path = os.path.join(tests_dir, 'webpages')
        with ZipFile(file_path) as file:
            file.extractall(extract_path)

        cls.pages = get_pages(extract_path)

    def test_get_title(self):
        titles = []
        for i, page in enumerate(TestHtmlParser.pages):
            parser = HtmlParser(page)
            title = parser.get_title()
            titles.append(title)
        self.assertCountEqual(titles, EXPECTED_TITLES)

    def test_get_links(self):
        link_url_hashes = []
        link_text_hashes = []
        for i, page in enumerate(TestHtmlParser.pages):
            parser = HtmlParser(page)
            links = parser.get_links()
            link_url_hashes_ = []
            link_text_hashes_ = []
            for url, text in links:
                link_url_hashes_.append(url)
                link_text_hashes_.append(text)
            link_url_hashes.append(unique_id(''.join(link_url_hashes_)))
            link_text_hashes.append(unique_id(''.join(link_text_hashes_)))
        self.assertCountEqual(link_url_hashes, EXPECTED_LINK_URL_HASHES)
        self.assertCountEqual(link_text_hashes, EXPECTED_LINK_TEXT_HASHES)

    def test_get_full_text(self):
        full_text_hashes = []
        for i, page in enumerate(TestHtmlParser.pages):
            parser = HtmlParser(page)
            text = parser.get_full_text()
            full_text_hashes.append(unique_id(text))
        self.assertCountEqual(full_text_hashes, EXPECTED_FULL_TEXT_HASHES)

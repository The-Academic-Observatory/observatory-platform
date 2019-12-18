import os
import unittest
import urllib.request
from zipfile import ZipFile

from academic_observatory.utils import HtmlParser
from academic_observatory.utils import unique_id

EXPECTED_TITLE_HASHES = ['f6358196a79e102796cd2524cfd12bce', 'be4e533fb04618234a2a3edda55d5723',
                         '1648a461b21b594b28df850d83d81cad', 'b2f6e383e334c4c3606f9e8b40b5d8ec',
                         'a28f5d5baa17e1e43d0c8a35b711afd5', 'ac8c9e2823099567388737b95aca40f5',
                         '5188c37d73c700a74233e7bbc8658ae4', '00443fe48c0f2219bc93f44205f1061b',
                         '788066d06719328205bf1aa9040f0f3b', 'd41d8cd98f00b204e9800998ecf8427e',
                         'f09492084e71548c217f6a706e70f78c', '13a95b433c40e5129005bf4e3f803c31',
                         'c482e522b1e7161845e2ffba8c5306bd', '2a2748d9c33bd8bcbaba983396d95b8c',
                         '335d74b4b8a746f6b13a8cc14cf81116', 'd41d8cd98f00b204e9800998ecf8427e',
                         '135250b93d6c758c9692d31e3bfdc043', '44835c940ffe8403128a97ddf0da1969',
                         '7384c330ddfc579fbefe95265d5d3703', '71344f993b0e45e703615a076c86965a',
                         '796678c24c50c1397bc793ec8338f39e', '021d5f097368b14dea39431a13ef8297',
                         '27eb0b5e2d8f9fde7af4db9ee0f2d663', '3ab721742ccbde5fa3cad852a41ddfd8',
                         '8fc471107f277c4befe8c6a22ae8aa99', '4f33c79e8fd9dd6d4248fbc36c74ed1c',
                         '56aa6885eaf38ac650991ff0f9a41b39', '8b8ac8f4ace00873ca6578372a417657',
                         '8c0014d8df3770994e7a30ed45b275f0', '6c4e379df111247329e30c94ee364ecb',
                         '36c4eb7e9ec9e8ede9becfe66a0f595c', '07a191bf94f8e578556f29e199ddbbd9',
                         '0427f09d74972e17bf708df3d9488aa1', '417a140cedbf24d3a728e42b21fc283c',
                         '32725b79a4af3a5323a85838e76d966d', '32f940d4ac440a38c77d79d9e94c65d2',
                         'd41d8cd98f00b204e9800998ecf8427e', 'd323cfdb2cb18d5c1b6961c8e10153b6',
                         'eacb2c275fd43055af9c85478fb737ef', 'd41d8cd98f00b204e9800998ecf8427e',
                         '0c64e24b70a7472c2651ee00715c470a', 'b8e0d341680e268ceb5142e3b5aabeab',
                         'd894a7abd3b9cdad6dda362ad14c9403', '7384c330ddfc579fbefe95265d5d3703',
                         '05d9299de0a22c613cb03333c3f42ee2', 'c3071513307065a3ed8f33b56e09123e',
                         '489cf57ec6933a3f4e27c6560a8ca5e4', '274279399696abf9af76cb2edf214e55',
                         'e61925cf20e88e65d1924c86de35d999', '8b634143957967135cbeff40497ec5d2',
                         'dacd3598c32f54a51a367ea072e2beb3', '05f420c304239a1787e0557762a79598',
                         '065b8061c44910475029f917fa8e272d', 'dea9ee7568b4f2310da0d9fbe50bf9e4',
                         '91ac28573efd0632127a3f253a201a53', '95096bb18c67ab034120d3c8350ea56f',
                         '4f33c79e8fd9dd6d4248fbc36c74ed1c', '5c804240063f6cf22c9eb45bf23bd2e7',
                         '58013d0b02e7287658c9aeef133525d6', '77b2d0506eb4672e52b9d6a0c810c1f4',
                         '1b06cf46d0d892b211f008988eff29ce', '04a6911dc02ed28e1986b44b8ff87c0d',
                         '01ac0ba9989e74ad3eb1bd8c56f8520a', '4fc682df97db57604745ba25cfb2036b',
                         '144b8e74c866a666856911be02af3f59', '5b6a9e719001ab94a9b4c50ba8474697',
                         '4fa9bcfca0258a8c34c5bf0cc87bbe1b', 'e042d704a1f9e7bab8887ff9cd7aba0d',
                         '8cf04a9734132302f96da8e113e80ce5', 'b3910547e40a74ae54ae9f52bd2e1a7e',
                         'fbcedf77257a6b557e06eee434184505', '5fb0caa55a8b0ee38f6494d16482a24e',
                         'cc14314d677160b08c6354d41d8094de', '06f05bafa21bf698954059eccb408587',
                         'd7db674fde0652e855da7b5828af3223', '10424105100d17693aabab7061b2fb76',
                         'ce5bbc9f7aea15d0488350d077fedfe9', '17a64090894f321d43a2b8a14cecf151',
                         '80c43ccdad55fb4c4f93f1e03ecf4f4a', '44393fb908db0b3665c6bf00f6665690',
                         'd69917b3fdd73d4845e78f3132833960', '9d04d005650f13d4ce45abb30b1a6770',
                         '240e13c6d6ad1a1e54f99f5115adca68', 'b46bb9f0ead86ede8269da380e651c8b',
                         '4f33c79e8fd9dd6d4248fbc36c74ed1c', 'b6adb44f29c3232548ee14dadb6612ac',
                         '94c40443d4db826dbb8309dd5b117a6d', '55b66043058e9c357e52e41184b1f3ad',
                         '482fbcf76f0ef581edcad815296410e5', 'ecf62237b6c314aab80ac218a20bb9d9',
                         'd41c4d0a29c4dbd64908290232d91e15', '28f54d06e9b073c834a9cc73328b3a22',
                         '4f30148c64174c428b74c888631d321c', '5fb0caa55a8b0ee38f6494d16482a24e',
                         'fbf24f824893ea39792492fe1c5f0283', '8cbe7f21a3073ccccf7f4a7cb0b774aa',
                         '574d3fb7354f51bb783f092b6700aa68', '88a4569a34529814cd3260e8c35aa2f1',
                         'e26066fb0c40646218a702520385d3f5']

EXPECTED_LINK_URL_HASHES = ['8d2446cae5c9fc86b8de83c8534e2d59', '64fb2280f0646d800b652da373516cdc',
                            '4ad91b91fd81fd172a7b9f822d687da7', 'd6f4bdf12816d782daf952c1e516b9b6',
                            'e402540d2f89acf7b34e70a180c47624', '2d300ed33bd21c51ed3dc2d9cfaf98ef',
                            'c4e01f591e14099fe38a01e835ea58b3', 'a714f86e59107ce0c5822022b53e08ff',
                            '1493a644d435820a3e69e816743214f6', 'd41d8cd98f00b204e9800998ecf8427e',
                            '5664041db79bf4d331a322686600618e', 'eda108a20c0b029d0669450390aa5bab',
                            '67a89522202f952e1579b0f5bb52e2d8', 'c09feeee33ea09a94e0e6c43c2fabdfe',
                            '6852da809011ca54e6f0ed935967c64e', 'd41d8cd98f00b204e9800998ecf8427e',
                            'ada5314efcbac2936da001e8f1a19c6a', 'e4100ff075f988e264d3c4a85e38c8ff',
                            '903d768a345ccf3635d8adb8a19ae7d2', '950df258eae367bc0722e4de548618e0',
                            'eda108a20c0b029d0669450390aa5bab', 'fd596afd9fcd16c97eb2d0473db17bb2',
                            '910a3fe60810f6b77883d7106eab20a3', '95ceaf67afe1f137f4183acf090fd26c',
                            '158a0ce7613ecb5750a209f7037b93ca', '77a23fa4be1609494555bb46cb806284',
                            'd41d8cd98f00b204e9800998ecf8427e', '83af93bbb95e3fd90fc9d2b779ac1e91',
                            'b8c48b33b670d8a513b1b175fd5f89d5', 'd5548997b063b59e526635ffed791732',
                            '4efa8f0717c72b3a32316289d8cd0308', 'd11a182d86a255f1936e897815c94534',
                            'cf26c4928b8f8ddab3ac73e7aed51049', '864697831fe803ec0c07bf348d957cb7',
                            '17b952ffec6c7f76fc06b2ae11942ee1', 'd41d8cd98f00b204e9800998ecf8427e',
                            'd41d8cd98f00b204e9800998ecf8427e', 'a779f4200f6c1fcd2d0f9e94ad1aeef0',
                            '84e758f9ea66059cc9436e48480269f1', 'd41d8cd98f00b204e9800998ecf8427e',
                            'd41d8cd98f00b204e9800998ecf8427e', '87cf77f548ae27a99a5d81744966b624',
                            '8eef591584b73f4b01ff018264d1a308', '903d768a345ccf3635d8adb8a19ae7d2',
                            'ba12c464419e9265b24d863e0c2194eb', '4e460fc507e08596cac41fefcebc112e',
                            '18c0cb841c76a97dbc229e9bb3ce9688', '3aac9e5a06de523190d15eb0fead153d',
                            '24f633f36b1d83d9cce4eb94b483f070', '1f3ef21ac68792dfb725ed0ec7510d79',
                            'c6f63f44e90f1626ffdc9a6a695262dc', 'd0c001b01d1eaf329dd4a9dcee3d098a',
                            '5a42371257a86815d07c6b0281f5a440', '06556e4d84eec66b5d4421d7e3d764d4',
                            '6fa096faf97a21cb1be108dcfb65ba66', '4e306fcfd89c7caaec9e700a701fcbfa',
                            '8b0853795e6b81e3db47ea853a0e4e91', '24ec6827eda8f67f6c3241f09aeb02c1',
                            '2078e0dfab4265ac6ebcb796bdfe0cbb', '83af93bbb95e3fd90fc9d2b779ac1e91',
                            '51bad858315c3bc0a719daedbc64e56f', '3ac8fed329ba372e7b1a44e7a1844ee1',
                            '2cead8319fc1d20373baef5ce0532b33', '91d049354a40672421c27a74deebda72',
                            '586cd188fd8bc2ecfe41a5b5fdf4d219', '9172881cc2fe8c15fbada2abe43af14a',
                            'cfd616a25e82549f1bd50a7b08b7f81c', 'd41d8cd98f00b204e9800998ecf8427e',
                            '3097be07da6fbe7798283d703b0fde12', '72cc29458ca0e9a156510ae74d682442',
                            'cd75e3c12998ac0f1b14ab7e72bdd47a', '2871e683f2bf1e35a005960a634d34c4',
                            '88ea6476e25700f1fa0e2ee310f28314', 'd57c22e30b2b8ceb37099ebe7d6522e5',
                            '97f13ed9d7fb51d9552a4b5225a09b08', '4cc97626541ca5512f3a7e75dddde8dc',
                            '58fd1884a5fbf6b54d5ea19b29c786db', '5b7094ad00946a0694f0270df8256da5',
                            'd1b11d7d174f1fdac70f1cbf91eb1fa0', '64915390a811ea2d884175c0f0e47d20',
                            'f36aa332658038e8120bbc1360adda4e', '67c4e8ecd83f00439bcc51045bef552e',
                            '1d066e1073ca456e8aa5b8faaee7c8e6', '31fe4751cf7a104d85a17f196ab0c448',
                            '0d72729579864e990fd82f698a39a39a', '32c89196a156c492370018a7595628f3',
                            '801c439f017613cc815c6b6a349e79da', '5c122363844342adaa70b63fe959bdc8',
                            '02cfeee14f04b4a12828b80150683823', '1e8be0aa91d163b3b98b4c09811225de',
                            '8713d8ccb3d64d2e1f0bbfb19ca72183', '974af5ab19d5def3cc1c68da010a5bd9',
                            '42bf73a3c831ea5352933124b2ed19f2', '2871e683f2bf1e35a005960a634d34c4',
                            '3a4c84c2a29d6a8b51921d20796f3935', '67c4e8ecd83f00439bcc51045bef552e',
                            '2f956cb956283541cb2463d1dc29b87f', 'cdd630bbbaa432509af6609050b37601',
                            '4288349c2daed3eb8fa9caf34a05b56f']

EXPECTED_LINK_TEXT_HASHES = ['97d58dd5a56a75071ab34f6a3b5c055e', '9ef7902cb8f867bcc71a6569c5a40c2e',
                             'c9010eaa20f97201cdb6ef2c277349ab', '694b8dff6a4935320f66750f9e7d53cd',
                             '8af87cf2eade312460e5185e8cfddd1a', 'bc82e9d96d588fbd2e41f8be2b9c5c11',
                             'addde3f0ecbbb153783d560142659c43', '368a3c73f4a51132ebb137c7bd245598',
                             'c1314bcd220279d4bb1d24ae977139cd', 'd41d8cd98f00b204e9800998ecf8427e',
                             'beab7287128e497ba36a9bf127339434', 'c21a54428c92ad3afd772286b82e9572',
                             '76956ee8fa3a79821b2c2ba4d8da0a09', 'bff946eecd0f2f9f7c3529c785cab43b',
                             '95572b99d56ade5faba5b0db541b1eac', 'd41d8cd98f00b204e9800998ecf8427e',
                             '844fd5e42194a37e3d671848e26648c9', '49ae701b93e16a06be6545edd92244ae',
                             '482abe76d2b0981c41f46e94dc693f68', '2ec5acf53fc59aadf2076bd56c8a23dd',
                             'c21a54428c92ad3afd772286b82e9572', 'e0a5bea4336de86289685204d38e827a',
                             'e76d211fe858d89176e6b4f76a48ef73', '94c5398ec8b445ae0addbdf5e0babe3c',
                             '6d4e28351815c0f3b971a3f7b6c99d89', '6c92285fa6d3e827b198d120ea3ac674',
                             'd41d8cd98f00b204e9800998ecf8427e', 'dc2044fcc9d5ed9c5c87dceee5154d74',
                             '4147cf1f439279040593fccc6ad8c8f3', '5e02f54f4ec64a0ac55e918a8e684df3',
                             '9b47fa831dec83ed19c16a6e9debade5', '69253739d34728db4c32310b34182fef',
                             'af87daf478aebb838c15dc1d18536199', '7d17158285ca52d2b7c8e1fbc5e3f8da',
                             '949a5f1c62b6735c326475dd7d9f0afc', 'd41d8cd98f00b204e9800998ecf8427e',
                             'd41d8cd98f00b204e9800998ecf8427e', 'b9ab05d80c6160be9733ea0e2f8a303f',
                             '725974c7834a15176c7dceb53ca012c9', 'd41d8cd98f00b204e9800998ecf8427e',
                             'd41d8cd98f00b204e9800998ecf8427e', '42383589b1064fed94ac6267528f66cd',
                             'bf1f15bb20a8309218ca6f1c50808ff7', '482abe76d2b0981c41f46e94dc693f68',
                             'f1e0f36999d29fad21963900122a8e48', '135be8542911b5c0fd969a46f2a1ba52',
                             '050cd0e468141056c5edb6ed63f392ee', '0c8649c9849d64dd972279233899efa3',
                             '468b589eb7110737222ddd69449457d4', '5ac14b20f83efea70ffac7e5212a4689',
                             'a683b60e3b4198517d0474d64fd332e2', '08fdfa83c9f8d8eb0cd2172c2e471b09',
                             '5689b4829c5ad556b5f0c1ebde55374e', '06556e4d84eec66b5d4421d7e3d764d4',
                             '057a3c0ca8006ec6a0395ac230c05536', 'ca59faba5d8035325fa31064c525d825',
                             '6c92285fa6d3e827b198d120ea3ac674', '201786137e97caca897f8cc062b946c7',
                             '49ea5e08cb58f151115138738cfd7380', 'dc2044fcc9d5ed9c5c87dceee5154d74',
                             'fec66b975a57010070f4ac998855020a', '6daa2dfcf90f3787436d0a30afc90480',
                             '3ced554e4b276d8d1125a8ed1bf81124', '36f0b53c7cdf49978b55c2f90044e174',
                             '072179ebc40804f142fff37a94b9a09a', 'ab80103e9ece94417142491e7e013590',
                             'd43566d9dd9dd8f97f942d4b3de78d46', 'd41d8cd98f00b204e9800998ecf8427e',
                             '180815c486511a10943179e1e294259c', '466617ebf15a1eaa05180ac895370b68',
                             '6c92285fa6d3e827b198d120ea3ac674', '8dfcfd235b86756e6a0f35d869afae01',
                             '83a65e31ea05d7b8964751e817cc8018', 'b2d25ea679adc78252a14b77810d6514',
                             '73f56fee2e72ebbf2479ca6b7931a41c', 'cc7e183e4a722a33c4893bbe08e9c959',
                             'fe573ff7b35cb98cec242f0c806a6a70', '80cdf3ddd6e4293af1fc8a5fac241de6',
                             'c5e3a78d7572e428ab0dbb9d2037fe85', 'd09ac24100d0f3758aa8943a085ad926',
                             'dd9d7aaed6aaa258ea09ffabad91ca10', 'd41d8cd98f00b204e9800998ecf8427e',
                             '275306fea83106ddb3eae109071c327a', '027c2531d6edbce2a75b679f0cb66343',
                             '6c92285fa6d3e827b198d120ea3ac674', 'a030a4984a58ba177e384d370c7abd8c',
                             'b8476e2710681afd32582a97b62f036e', '3ff18ac2cd1cc777825b980ebb66cf9f',
                             '932658e2a3628661c30a0a37c9538e00', '99dd1e3bfc4b239b15b779d2694926e3',
                             'aba3b35707a2da39d8f3ff4ace8aebdf', '7ad5d0b706ff5a14d762298ba56d7195',
                             '9c99b4192202f43917e6c5a401f516f3', '8dfcfd235b86756e6a0f35d869afae01',
                             '6cf0e0de2bbc716f69fc901036dab4c1', 'd41d8cd98f00b204e9800998ecf8427e',
                             '3e7dd208b463b2f823362c58c88d7632', 'd634af67b13feca4fd6039a0f2139440',
                             '7fffdc5d0d83ae9338072d9b48eccf04']

EXPECTED_FULL_TEXT_HASHES = ['e61ba76eb2d32db88b38d4106bc7509e', '00e63b3ac4135be79edcc5f7f8a449ee',
                             'e97dfee25d049ea4bb710a8ae9c5c73b', '558f95a4726f5623711cefc226453556',
                             '3c6918e1d278801f22c262ab0e0a4e51', '0e34b2a4cde1a50e34d5c145b2ff4b08',
                             '1666d1e5cdc661918cb34c45153cccb4', '475fa696c516c2023c8fb4d5ba64a314',
                             '52e89d33ac6ec5bef60884f3c3990f36', '515c99044e5a21629cbc1ea11bcd814b',
                             '83ae9a0ce094d4c5f8d608a200843448', 'bd30727d4a3535710b9e98080605412d',
                             'a1eb9402a2f38ae531c2fd7b22db574c', '9a11011554c6f5676929560071a6755e',
                             '76ea674324345c6e34811ac807246b2b', '96c5637e1eb8f8f8c34172f2d23eafc6',
                             'd769bff6c7932088df36d1c2d483ee04', '4776d1017cad86814f3c85d90bb2e868',
                             '9576fd4213f496d7873120d138d52324', '44cda7da2040082334f17b5514a60ce5',
                             '1dd4309776ed88a4d7f16b0df74cb69c', '87101dd523c3587c6e246e62c04696d3',
                             'b233350c6dfcc0ffc1a2ed610e568aa9', '7d0062c64b579057707b886b0c2eedc0',
                             '3c68f719eafe05eac55ba81ad708c762', 'da41af028fc3e88466e13821bd37fc1d',
                             '33a75d73a429cdc1be2f4b2259efc714', '10e9daa904d22b1b2ea05ff43f536fb9',
                             '27ceeb9c8db040178632a098f1fad790', '521bf3c72c1b9521814e001b971ba750',
                             'fdf8e284062938be1d8455b7bcf212a8', '8fb2af88e3d812cfdf01ad6f48ca5bec',
                             '8420e03908435a150ec8e34ddb41530e', '8fa4ff3d1fc1741dcc31bd7f72cf8ed3',
                             '71ff1abd530eeb62502da490ff908c95', '7243a0b78a11f64a0b444a98b37c3acd',
                             'd41d8cd98f00b204e9800998ecf8427e', 'b51aaba41ed2bb32b6b88b41e2a3db24',
                             '7d3d14b7545e1b97aa452b287e39dfba', 'd41d8cd98f00b204e9800998ecf8427e',
                             'c1bd9e4166ba38d12e1e93ea0da4e7e1', 'b4f627fe7721a99f37114327de0508f5',
                             '0c1debe1fc0a9ae8d1712d8813ef7d4a', '9576fd4213f496d7873120d138d52324',
                             '10ca0ff4a85981b40ba58317ad923dcd', '0de993493a1123475054469e18e90e25',
                             'f8768164c43f73026503d0ec926fc599', '6bc214487ef98b79cae1b60d7d638429',
                             'c2276b4f262603a834b661976429c595', '7a182cf8a6829fde05bbe24de71b25d3',
                             'a79eda9917e61a833324d0243445e682', '3e75f227b3d8132ea71985cd53c8a52b',
                             'b51a1edc72a84e843637410a68211c55', '2c864432fbe9a793094c6785a8d23615',
                             'bfd9ce352a05b3d3b81051a9e760fa78', 'e8072b6ba14e4b37b038f5ee3007d123',
                             'da41af028fc3e88466e13821bd37fc1d', 'b71c598e81deff8a01d387ca140d597a',
                             '48ab5cb3b730cae374f1a2523ae3effe', '19a39294b391513cd3994ec174e54958',
                             '72291b29eac488bb7e65548f2a444567', '0e8191bc8828d44190ed3bb4ba976451',
                             '6e698b047d2a6c4e13e74325ed002370', 'c4ce235b73702d0f2f4850d98b6758ff',
                             '9eff35dc5dce7da0a8e32da609b82c79', '187958f35217f66aa3b172be6c79ae35',
                             '626edc2d23ab4264a4dfa16e6588a1d7', '580bece0595886a319e5c4ffeade0a63',
                             '735e7eb387b92ef766ed9d410394bbf7', '6de4c454df57093d7d0705c7c5422f35',
                             'e61980c17836258c279bd3dba536be7b', 'e06e2ae93d6644f08967c273ea4d318f',
                             '7a28905c69020aa3132e14e2a4d712de', '2f219b5487fc51c5269ff1b54aac0353',
                             'f1b6cfd22e7854b4b625270a36075c9e', '7cd72648fe43ebcd8ab3a74b970461e2',
                             'cfcea9a38923ba8166f6c2fa099157d7', '673b68f58828a700701601e446535389',
                             '14bbc281d3934d7b7b6a46d7c8fd0a7a', 'c5d804ac163cbab75ab673ec6a8a47d2',
                             '2f77e929cdb51bc4b68ca08e2de515f8', '4adf02a5e616d02d6913eaedd7173f06',
                             '9d84cba68e33dce22a661b8769bc9a1f', '3d3ca7f88154a08dabd8e13f0f3cc1d5',
                             'da41af028fc3e88466e13821bd37fc1d', '59ed090e9ba5742e9f9fa9b1e9d907c0',
                             '63dd1593cad65629fc52fca1a9f0ecff', '105ffb829763fe196eb4029c385aa782',
                             '8fbdbaaac918e544e9c8e4bcfce26f10', 'b230b5c1c5c1216d2d2346674eef80c6',
                             '609b9cf663a351bc6fca044088bca47c', '61ed87842a4f0c20a03e6c9634045117',
                             '7fbedd4fc3a832633315b2f02b367fef', 'e06e2ae93d6644f08967c273ea4d318f',
                             '2da01c1dcd82df154a60e4008012e41b', 'ffcf3a243578bbcad08081325b1325fd',
                             '0eaa42d7af75c5ea0a77ce427c27cb76', 'cb79d51e7ea4f935d4682d1e2d211ba0',
                             'd5b401df14c4a0d7f0e99c461fa5960f']


def download(url):
    # Create data folder
    save_folder = 'data'
    if not os.path.exists(save_folder):
        os.mkdir(save_folder)

    # Download
    save_path = os.path.join(save_folder, 'webpages.zip')
    urllib.request.urlretrieve(url, save_path)

    # Unzip
    extract_path = os.path.join(save_folder, 'webpages')
    with ZipFile(save_path) as file:
        file.extractall(extract_path)


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


class TesHtmlParser(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TesHtmlParser, self).__init__(*args, **kwargs)
        self.test_file_location = 'data/webpages'
        self.test_file_url = 'https://onedrive.live.com/download?cid=6917C8254765425B&resid=6917C8254765425B%21154&authkey=AMSit72vXzUxpqI'

        if not os.path.exists(self.test_file_location):
            download(self.test_file_url)
        self.pages = get_pages(self.test_file_location)

    def test_get_title(self):
        title_hashes = []
        for i, page in enumerate(self.pages):
            parser = HtmlParser(page)
            title = parser.get_title()
            title_hashes.append(unique_id(title))
        self.assertListEqual(title_hashes, EXPECTED_TITLE_HASHES)

    # def test_get_links(self):
    #     link_url_hashes = []
    #     link_text_hashes = []
    #     for i, page in enumerate(self.pages):
    #         parser = HtmlParser(page)
    #         links = parser.get_links()
    #         link_url_hashes_ = []
    #         link_text_hashes_ = []
    #         for url, text in links:
    #             link_url_hashes_.append(url)
    #             link_text_hashes_.append(text)
    #         link_url_hashes.append(unique_id(''.join(link_url_hashes_)))
    #         link_text_hashes.append(unique_id(''.join(link_text_hashes_)))
    #     self.assertListEqual(link_url_hashes, EXPECTED_LINK_URL_HASHES)
    #     self.assertListEqual(link_text_hashes, EXPECTED_LINK_TEXT_HASHES)

    def test_get_full_text(self):
        full_text_hashes = []
        for i, page in enumerate(self.pages):
            parser = HtmlParser(page)
            text = parser.get_full_text()
            full_text_hashes.append(unique_id(text))
        self.assertListEqual(full_text_hashes, EXPECTED_FULL_TEXT_HASHES)

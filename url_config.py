urls = {
    # Sentinel-1,2 from main url
    'Sentinel-1':  # zip: folder with folders
        {
            'url_search': "https://scihub.copernicus.eu/dhus/search?q=",
            'auth': ('', ''),
            'url_download': "https://scihub.copernicus.eu/dhus/odata/v1/Products('{}')/$value",
            'url_md5': "https://scihub.copernicus.eu/dhus/odata/v1/Products('{}')/Checksum/Value/$value",
            'platformname': 'Sentinel-1',
            'producttype': "GRD"
        },
    'Sentinel-2':
        {
            'url_search': "https://scihub.copernicus.eu/dhus/search?q=",
            'auth': ('', ''),
            'url_download': "https://scihub.copernicus.eu/dhus/odata/v1/Products('{}')/$value",
            'url_md5': "https://scihub.copernicus.eu/dhus/odata/v1/Products('{}')/Checksum/Value/$value",
            'platformname': 'Sentinel-2',
            'producttype': "S2MSI1C"
        },
    'Sentinel-3_pre':  # zip: folder with .nc
        {
            'url_search': "https://scihub.copernicus.eu/s3/search?q=",
            'auth': ('s3guest', 's3guest'),
            'url_download': "https://scihub.copernicus.eu/s3/odata/v1/Products('{}')/$value",
            'url_md5': "https://scihub.copernicus.eu/s3/odata/v1/Products('{}')/Checksum/Value/$value",
            'platformname': 'Sentinel-3',
            'productlevel': 'L1',
            'producttype': "OL_1_EFR___"
        },
    'Sentinel-3':
        {
            'url_search': "https://scihub.copernicus.eu/dhus/search?q=",
            'auth': ('', ''),
            'url_download': "https://scihub.copernicus.eu/dhus/odata/v1/Products('{}')/$value",
            'url_md5': "https://scihub.copernicus.eu/dhus/odata/v1/Products('{}')/Checksum/Value/$value",
            'platformname': 'Sentinel-3',
            'productlevel': 'L1',
            'producttype': "OL_1_EFR___"
        },
    'Sentinel-5':  # single .nc file
        {
            'url_search': "https://s5phub.copernicus.eu/dhus/search?q=",
            'auth': ('s5pguest', 's5pguest'),
            'url_download': "https://s5phub.copernicus.eu/dhus/odata/v1/Products('{}')/$value",
            'url_md5': "https://s5phub.copernicus.eu/dhus/odata/v1/Products('{}')/Checksum/Value/$value",
            'platformname': 'Sentinel-5',
            'processinglevel': 'L1B',
            'producttype': "L1B_IR_SIR"
        }
}


def get_urls_and_query(platform_name):
    url_dict = urls[platform_name]
    query_template = '(footprint:"Intersects({{polygon}})") AND ' \
                     '(beginPosition:[{{date_start}}T00:00:00.000Z TO {{date_end}}T23:59:59.999Z] AND ' \
                     'endPosition:[{{date_start}}T00:00:00.000Z TO {{date_end}}T23:59:59.999Z] ) AND ' \
                     '(platformname:{platform_name} AND {{level_or_type}})&rows=100&start={{start}}'.format(platform_name=platform_name)
    return url_dict, query_template

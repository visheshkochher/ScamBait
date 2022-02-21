import os
import json
import re
import concurrent
import time
import requests
import pandas as pd
from google.cloud import vision
from _secrets import data

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = data.FULL_PATH_EXT+"ScamBait/_secrets/scambait_gauth.json"

# CONSTANTS
API_KEY = data.API_KEY
phone_regex_pattern = '''((?:\+\d{2}[-\.\s]??|\d{4}[-\.\s]??)?(?:\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4}))'''
MAX_PAGES = 3

POSTCODES = pd.read_csv(data.FULL_PATH_EXT + 'ScamBait/postcodes_IN.txt', sep='\t', header=None)
POSTCODES.columns = ['Country', 'Postcode', 'Circle', 'State',
                     'StateID', 'District', 'Something', 'CitySomething', 'Something2', 'Latitude', 'Longitude',
                     'Category']
POSTCODES.District = POSTCODES['District'].astype('category')


def get_url_content(url, return_text=True):
    """Fetch url response
    """
    content = requests.get(url)
    text = json.loads(content.text)
    return text if return_text else content


def get_candidate_details(candidate_list):
    """Iterate over candidates of a Place findplacefromtext API result
    """
    results = []

    def candidate_detail_fetch(candidate):
        # for candidate in candidate_list:
        url = 'https://maps.googleapis.com/maps/api/place/details/json?place_id={}&key={}' \
            .format(candidate['place_id'], API_KEY)
        print(url)
        details = get_url_content(url)
        results.extend([details])

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = [executor.submit(candidate_detail_fetch, candidate) for i, candidate in
                         candidate_list.iterrows()]

    return results


def detect_text_uri(uri):
    """Detects text in the file located in Google Cloud Storage or on the Web.
    """
    client = vision.ImageAnnotatorClient()
    image = vision.Image()
    image.source.image_uri = uri

    response = client.text_detection(image=image)

    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))

    return response


# SEARCH WINE SHOPS IN AN AREA WITH QUERY
if __name__ == '__main__':
    # CITY = input("Enter City Name:")
    search_query = 'wine%20shop'
    search_district = 'Delhi'
    district_listings = []
    for i, postcode in POSTCODES.query("State=='%s'" % search_district).iterrows():
        query_url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query={}&location={}%2C{}&radius=5000&key={}' \
            .format(
            search_query, str(postcode['Latitude']), str(postcode['Longitude']), API_KEY)
        print(query_url)
        query_data = get_url_content(query_url)
        listings = query_data['results']
        district_listings.extend(listings)
        token = query_data['next_page_token']
        time.sleep(0.2)
        # PAGINATE AND EXTEND LISTINGS

        counter = 0
        while counter < MAX_PAGES:
            print(counter, token)
            query_data = get_url_content(query_url + '&pagetoken=' + token)
            new_listings = query_data['results']
            # listings.extend(new_listings)
            district_listings.extend(new_listings)
            if 'next_page_token' in query_data.keys():
                token = query_data['next_page_token']
                time.sleep(0.2)
                counter += 1
            else:
                break

    district_listings_df = pd.DataFrame(district_listings)
    district_listings_df.drop_duplicates(subset='place_id', inplace=True)
    # GET DETAILS OF ALL THESE LISTINGS
    listing_details = get_candidate_details(district_listings_df)

    # GET PHOTO URL FOR EACH OF THE LISTINGS
    photo_url_list = {}
    for listing in listing_details:
        try:
            listing_photos = []
            if 'photos' in listing['result'].keys() and 'formatted_phone_number' not in listing['result'].keys():
                for photo in listing['result']['photos']:
                    photo_url = '''https://maps.googleapis.com/maps/api/place/photo?maxwidth=1600&photo_reference={}&key={}'''.format(
                        photo['photo_reference'], API_KEY)
                    listing_photos.append(photo_url)
                photo_url_list[listing['result']['place_id']] = listing_photos
        except Exception as e:
            pass
            print("Error in photo_url_list: {}".format(e))

    # GET TEXT ON EACH LISTINGS PHOTOS
    # url_text_results = {}
    url_text_list = []


    def detect_listing_photos(items_tuple):
        # for listing, this_photo_urls in photo_url_list.items():
        listing, this_photo_urls = items_tuple
        listing_results = []
        for photo_url in this_photo_urls[:3]:
            print(photo_url)
            try:
                detection_result = detect_text_uri(photo_url)
                if detection_result.text_annotations:
                    photo_text = [' {} '.format(x.description) for x in detection_result.text_annotations][0]
                    phone_number_listed = re.findall(phone_regex_pattern, photo_text)
                    has_phone_number = True if phone_number_listed else False
                    listing_results.append((photo_url, photo_text, phone_number_listed, has_phone_number))
            except Exception as e:
                print(e)
                continue
        # url_text_results[listing] = listing_results
        url_text_list.extend([{'place_id': listing, 'results': listing_results}])


    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = [executor.submit(detect_listing_photos, candidate) for candidate in photo_url_list.items()]

    # GET ALL DATA IN DATAFRAMES AND JOIN THEM IN ONE TOTAL_DF_CLEAN
    url_text_formatted = []
    # for place_id, photo_list in url_text_results.items():
    for item in url_text_list:
        place_id = item['place_id']
        photo_list = item['results']
        for photo in photo_list:
            info_dict = {'place_id': place_id,
                         'photo_url': photo[0],
                         'photo_text': photo[1].replace('\n', ' '),
                         'photo_mobile_number': photo[2],
                         'photo_has_number': photo[3]}
            url_text_formatted.append(info_dict)
    url_text_results_df = pd.DataFrame(url_text_formatted)
    listing_details_df = pd.DataFrame([x['result'] for x in listing_details])
    listings_df = pd.DataFrame(listings)

    url_text_results_df.to_csv(
        data.FULL_PATH_EXT + 'ScamBait/data/url_text_data_%s.csv' % search_query.replace('%20', '_'),
        index=False, sep=';')
    listing_details_df.to_csv(
        data.FULL_PATH_EXT + 'ScamBait/data/listing_details_df_%s.csv' % search_query.replace('%20',
                                                                                              '_'),
        index=False, sep=';')

    total_df = listing_details_df.merge(url_text_results_df)
    total_df_clean = total_df[
        ['name', 'photo_mobile_number', 'photo_text', 'url', 'photo_url', 'photos', 'place_id', 'formatted_address']]
    total_df_clean.to_csv(
        data.FULL_PATH_EXT + 'ScamBait/data/%s.csv' % search_query.replace('%20', '_'), index=False,
        sep=';')

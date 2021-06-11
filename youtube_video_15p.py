import requests, sys, time, os, argparse
import schedule

# Any characters to exclude, generally these are things that become problematic in CSV files
unsafe_characters = ['\n', '"']

def setup(api_path):
    with open(api_path, 'r') as file:
        api_key = file.readline()

    return api_key


def prepare_feature(feature):
    # Removes any character from the unsafe characters list and surrounds the whole item in quotes
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'


def api_request(page_token):
    # Builds the URL and requests the JSON from it
    request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,contentDetails,snippet{page_token}id=d26cqi0qAks&key={api_key}"

    # request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,contentDetails,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
    request = requests.get(request_url)
    if request.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        sys.exit()
    return request.json()


def get_tags(tags_list):
    # Takes a list of tags, prepares each tag and joins them into a string by the pipe character
    return prepare_feature("|".join(tags_list))


def get_videos(items):
    lines = []
    for video in items:
        comments_disabled = False
        ratings_disabled = False

        # We can assume something is wrong with the video if it has no statistics, often this means it has been deleted
        # so we can just skip it
        if "statistics" not in video:
            continue

        # A full explanation of all of these features can be found on the GitHub page for this project
        video_id = prepare_feature(video['id'])

        # Snippet and statistics are sub-dicts of video, containing the most useful info
        snippet = video['snippet']
        statistics = video['statistics']
        contentDetails = video['contentDetails']

        # The following are special case features which require unique processing, or are not within the snippet dict
        #description = snippet.get("description", "")
        trending_date = time.strftime("%b %d %Y %H:%M:%S")
        view_count = statistics.get("viewCount", 0)

        

        # Compiles all of the various bits of info into one consistently formatted line
        line = [video_id] + [prepare_feature(x) for x in [trending_date, view_count]]
        lines.append(",".join(line))
    return lines


def get_pages(next_page_token="&"):
    video_data = []

    # Because the API uses page tokens (which are literally just the same function of numbers everywhere) it is much
    # more inconvenient to iterate over pages, but that is what is done here.
    while next_page_token is not None:
        # A page of data i.e. a list of videos and all needed data
        video_data_page = api_request(next_page_token)

        # Get the next page token and build a string which can be injected into the request with it, unless it's None,
        # then let the whole thing be None so that the loop ends after this cycle
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token

        # Get all of the items as a list and let get_videos return the needed features
        items = video_data_page.get('items', [])
        video_data += get_videos(items)

    return video_data


def write_to_file(video_data):

    print(f"Writing data to file...")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(f"{output_dir}/videos.csv", "a", encoding='utf-8') as file:
        for row in video_data:
            file.write(f"{row}\n")


def get_data():
    
    video_data = get_pages()
    write_to_file(video_data)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--key_path', help='Path to the file containing the api key, by default will use api_key.txt in the same directory', default='api_key.txt')
    parser.add_argument('--output_dir', help='Path to save the outputted files in', default='output15p/')

    args = parser.parse_args()

    output_dir = args.output_dir
    api_key= setup(args.key_path)

    get_data()

    schedule.every().minutes.do(get_data)

    while True:
        schedule.run_pending()
        time.sleep(1)
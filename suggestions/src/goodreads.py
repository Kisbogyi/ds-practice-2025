import requests 
import Levenshtein
import re
import json
from typing import Optional, Tuple
from book import Book

def get_book_url(title: str, goodreads_base_url: str) -> Optional[str]:
    payload = {'query': title}
    # They validate user agents
    headers = {'user-agent': 'curl/8.18.0'}
    r = requests.get(f"{goodreads_base_url}/search", params=payload, headers=headers)
    response: str = r.text
    p = re.compile(r"class=\"bookTitle\".*href=\"(?P<url>.*)\?.*\".*")
    ms = p.finditer(response)

    urls = [match.group("url") for match in ms]
    if len(urls) < 1:
        return
    best_match = sorted(urls, key=lambda url: Levenshtein.distance(url, title))[0]
    return best_match

def get_graphql_js_url(book_url: str, goodreads_base_url: str) -> str:
    r2 = requests.get(f"{goodreads_base_url}{book_url}")
    response = r2.text
    p = re.compile(r"class=\"BookCard__title\">(?P<title>.*)<.*")
    ms = p.finditer(response)
    for m in ms:
        print(m.group("title"))
    
    p = re.compile(r"<script src=\"(?P<graphql_js>\/_next\/static\/chunks\/pages\/_app-.*?\.js)\" defer=\"\"><\/script>")
    ms = p.search(response)
    if ms is None:
        raise ValueError("Could not find graphql js url in site")
    return ms.group("graphql_js")

def get_graphql_info(graphql_js_url: str, goodreads_base_url: str) -> Tuple[str, str]:
    headers = {'user-agent': 'curl/8.18.0'}
    r3 = requests.get(f"{goodreads_base_url}{graphql_js_url}", headers=headers)
    response = r3.text

    p = re.compile(r"JSON.parse\(\'(?P<json>.*?)\'\)")
    ms = p.search(response)
    if ms is None:
        raise ValueError("Could not find graphql api key in file")
    json_string = ms.group("json")
    graphql_data = json.loads(json_string)
    api_key = graphql_data["Production"]["graphql"]["apiKey"]
    endpoint = graphql_data["Production"]["graphql"]["endpoint"]
    return api_key, endpoint

def graphql_suggestions_ep(api_key: str, endpoint_url: str) -> list[Book]:
    host = endpoint_url.removesuffix(r"/graphql").removeprefix(r"https://")
    print(endpoint_url)
    print(host)
    print(api_key)

    headers = {
        "Host": host,
        "X-Api-Key": api_key,
        "Content-Type": "application/json",
        "Origin": "https://www.goodreads.com",
        "Referer": "https://www.goodreads.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }

    payload = {
        "operationName": "getSimilarBooks",
        "variables": {
            "limit": 5,
            "id": "kca://book/amzn1.gr.book.v3.8a3MN-Tkitx5VA-h"
        },
        "query": """query getSimilarBooks($id: ID!, $limit: Int!) {
                      getSimilarBooks(id: $id, pagination: {limit: $limit}) {
                        webUrl
                        edges {
                          node {
                            title
                            imageUrl
                            webUrl
                            primaryContributorEdge {
                              node {
                                name
                                __typename
                              }
                              __typename
                            }
                            work {
                              stats {
                                averageRating
                                ratingsCount
                                __typename
                              }
                              __typename
                            }
                            __typename
                          }
                          __typename
                        }
                        __typename
                      }
                    }"""
    }

    # Requests handles compression (gzip/br) automatically
    response = requests.post(endpoint_url, headers=headers, json=payload, verify=False)
    if (response.status_code == 429):
        raise ValueError("API Quota reached")
    grpc_response = response.json()
    edges = grpc_response["data"]["getSimilarBooks"]["edges"]
    return [Book(edge["node"]["title"], edge["node"]["primaryContributorEdge"]["node"]["name"]) for edge in edges]


def get_recommendations(title: str) -> Optional[list[Book]]:
    goodreads_base_url = "https://www.goodreads.com"
    book_url = get_book_url(title, goodreads_base_url)

    if book_url is None:
        return 

    graphql_js_url = get_graphql_js_url(book_url, goodreads_base_url)
    print(graphql_js_url)
    
    api_key, endpoint = get_graphql_info(graphql_js_url, goodreads_base_url)
    return graphql_suggestions_ep(api_key, endpoint)

    
print(get_recommendations("Kushiel's Dart"))

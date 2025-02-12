from typing import TypedDict, List, Dict, Optional, Any
import requests
from bs4 import BeautifulSoup
import re
import json
from http.cookies import SimpleCookie


class Chapter(TypedDict):
    id: str
    title: str


class NewChapter(TypedDict):
    id: str
    storyData: str
    chapterData: str
    title: str
    images: List[str]
    pages: int
    chapters: List[Chapter]
    nextChapter: Optional[str]
    previousChapter: Optional[str]
    parentId: str
    parentTitle: str
    createdAt: str
    updatedAt: str


class Author(TypedDict):
    name: str
    id: str


class NewManga(TypedDict):
    id: str
    mangaId: str
    storyData: str
    imageUrl: str
    titles: Dict[str, str]
    authors: List[Author]
    status: str
    views: str
    score: float
    genres: List[str]
    description: str
    chapters: List[NewChapter]
    createdAt: str
    updatedAt: str


async def scrape_manga_chapter(
    manga_id: str, chapter_id: str, user_acc: str = None, server: str = "1"
) -> NewChapter:
    try:
        # Set up headers with cookies
        cookies = {
            "user_acc": user_acc if user_acc else "",
            "content_server": f"server{server}",
        }

        # Fetch the HTML content of the page
        response = requests.get(
            f"https://chapmanganato.to/{manga_id}/{chapter_id}", cookies=cookies
        )
        response.raise_for_status()

        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract breadcrumb information
        breadcrumb_links = soup.select(".panel-breadcrumb a")
        manga_title = breadcrumb_links[1].text
        parent = breadcrumb_links[1].get("href", "").split("/")[-1]
        chapter_title = breadcrumb_links[2].text

        # Extract chapters
        chapters = []
        chapter_select = soup.select_one(".navi-change-chapter")
        if chapter_select:
            for option in chapter_select.select("option"):
                chapter_value = option.get("data-c")
                if chapter_value:
                    chapters.append({"id": chapter_value, "title": option.text})

        # Extract images
        images = []
        image_elements = soup.select(".container-chapter-reader img")
        for img in image_elements:
            image_url = img.get("src")
            if image_url:
                images.append(image_url)

        pages = len(images)

        # Get next and previous chapter links
        next_chapter_link = soup.select_one(".navi-change-chapter-btn-next")
        next_chapter = (
            next_chapter_link.get("href", "").split("/")[-1]
            if next_chapter_link
            else None
        )

        last_chapter_link = soup.select_one(".navi-change-chapter-btn-prev")
        last_chapter = (
            last_chapter_link.get("href", "").split("/")[-1]
            if last_chapter_link
            else None
        )

        # Extract story and chapter data from scripts
        glb_story_data = ""
        glb_chapter_data = ""

        script_tags = soup.select(".body-site script")
        for script in script_tags:
            script_content = script.string
            if script_content and "glb_story_data" in script_content:
                story_match = re.search(
                    r"glb_story_data\s*=\s*'([^']+)'", script_content
                )
                chapter_match = re.search(
                    r"glb_chapter_data\s*=\s*'([^']+)'", script_content
                )

                if story_match:
                    glb_story_data = story_match.group(1)
                if chapter_match:
                    glb_chapter_data = chapter_match.group(1)

        data = {
            "id": chapter_id,
            "storyData": glb_story_data,
            "chapterData": glb_chapter_data,
            "title": chapter_title,
            "images": images,
            "pages": pages,
            "chapters": chapters,
            "nextChapter": next_chapter,
            "previousChapter": last_chapter,
            "parentId": parent,
            "parentTitle": manga_title,
            "createdAt": "",
            "updatedAt": "",
        }
        return data

    except requests.RequestException as e:
        raise Exception(f"Error fetching manga chapter: {str(e)}")


async def process_manga_list(url: str, page: str):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        manga_list = []
        popular = []

        # Process main manga list
        for manga_element in soup.select(".content-genres-item"):
            image_url = manga_element.select_one("img.img-loading").get("src", "")
            title_element = manga_element.select_one("h3 a.genres-item-name")
            title = title_element.text
            manga_url = title_element.get("href", "")

            chapter_element = manga_element.select_one("a.genres-item-chap")
            latest_chapter = ""
            if chapter_element and chapter_element.text:
                chapter_match = re.search(
                    r"[Cc]hapter\s(\d+(?:\.\d+)?)",
                    chapter_element.text.replace("-", "."),
                )
                if chapter_match:
                    latest_chapter = chapter_match.group(1)

            manga_list.append(
                {
                    "id": manga_url.split("/")[-1] if manga_url else "",
                    "image": image_url,
                    "title": title,
                    "chapter": latest_chapter,
                    "chapterUrl": chapter_element.get("href", "")
                    if chapter_element
                    else "",
                    "description": manga_element.select_one(
                        ".genres-item-description"
                    ).text.strip(),
                    "rating": manga_element.select_one("em.genres-item-rate").text,
                    "views": manga_element.select_one(".genres-item-view").text,
                    "date": manga_element.select_one(".genres-item-time").text,
                    "author": manga_element.select_one(".genres-item-author").text,
                }
            )

        # Process popular manga
        for manga_element in soup.select(".item"):
            image_url = manga_element.select_one("img.img-loading").get("src", "")
            title_element = manga_element.select_one("h3 a.text-nowrap")
            title = title_element.text
            manga_url = title_element.get("href", "")

            chapter_element = manga_element.select_one("a.text-nowrap")
            latest_chapter = ""
            if chapter_element:
                chapter_text = chapter_element.text.replace(title, "").replace("-", ".")
                chapter_match = re.search(r"[Cc]hapter\s(\d+(?:\.\d+)?)", chapter_text)
                if chapter_match:
                    latest_chapter = chapter_match.group(1)

            popular.append(
                {
                    "id": manga_url.split("/")[-1] if manga_url else "",
                    "image": image_url,
                    "title": title,
                    "description": "",
                    "chapter": latest_chapter,
                    "chapterUrl": chapter_element.get("href", "")
                    if chapter_element
                    else "",
                    "date": "",
                    "rating": "",
                    "views": "",
                    "author": "",
                }
            )

        # Get pagination info
        total_stories = len(manga_list)
        last_page_element = soup.select_one("a.page-last")
        total_pages = 1
        if last_page_element:
            page_match = re.search(r"\d+", last_page_element.text)
            if page_match:
                total_pages = int(page_match.group(0))

        if int(page) > total_pages:
            return {"error": "Page not found", "status_code": 404}

        return {
            "mangaList": manga_list,
            "popular": popular,
            "metaData": {"totalStories": total_stories, "totalPages": total_pages},
        }

    except requests.RequestException as e:
        raise Exception(f"Failed to fetch manga list: {str(e)}")


def scrape_manga_list(page: str = "1"):
    try:
        # Fetch new data if not in cache
        url = f"https://manganato.com/genre-all/{page}"
        result = process_manga_list(url, page)

        return result

    except Exception as e:
        return {
            "data": {"error": "Failed to fetch latest manga"},
            "status": 500,
            "headers": {"Content-Type": "application/json"},
        }

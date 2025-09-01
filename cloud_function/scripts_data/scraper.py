import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv, time, random, os
import pandas as pd
import hashlib
import logging

SCRAPER_MODE = os.getenv("SCRAPER_MODE", "csv")
BASE_URL = "https://fr.trustpilot.com"
START_URL = "https://fr.trustpilot.com/review/www.leroymerlin.fr"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36"
}

logger = logging.getLogger(__name__)

def generate_review_hash(row):
    """créer un hash unique basé sur auteur, contenu et date de publication"""
    key = f"{row['author']}|{row['content']}|{row['publication_date']}"
    return hashlib.md5(key.encode('utf-8')).hexdigest()

def scrape_reviews(mode=None, scrape_date=None):
    """fonction principale de scraping des avis. scrape_date est la date du jour au format ISO (YYYY-MM-DD); 
    cutoff_date est la date limite pour arrêter le scraping (7 jours avant la date du jour).
    mode peut être 'json', 'pandas' ou 'csv' (par défaut défini par la variable d'environnement SCRAPER_MODE)"""
    mode = mode or SCRAPER_MODE
    scrape_date = scrape_date or datetime.utcnow().date().isoformat()
    cutoff_date = datetime.utcnow().date() - timedelta(days=7)

    reviews_list = []
    current_url = START_URL
    logger.info("Début du scraping...")

    try:
        while current_url:
            logger.info(f"Scraping : {current_url}")
            response = requests.get(current_url, headers=HEADERS, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            reviews = soup.find_all('article', attrs={"data-service-review-card-paper": "true"})
            stop_scraping = False

            for review in reviews:
                rating_tag = review.find('div', attrs={"data-service-review-rating": True})
                comment_tag = review.find('p', attrs={"data-service-review-text-typography": True})
                author_tag = review.find("span", attrs={"data-consumer-name-typography": "true"})
                date_tag = review.find('time')

                rating = rating_tag['data-service-review-rating'] if rating_tag else None
                author = author_tag.text.strip() if author_tag else "Auteur inconnu"
                comment = comment_tag.get_text(separator="\n").strip() if comment_tag else None

                try:
                    publication_date = datetime.fromisoformat(date_tag['datetime'].replace('Z', '+00:00')).date()
                except:
                    continue

                if not rating or not comment or not publication_date:
                    continue
                if publication_date < cutoff_date:
                    stop_scraping = True
                    break

                review_dict = {
                    'review_id': generate_review_hash({
                        'author': author,
                        'content': comment,
                        'publication_date': publication_date.isoformat()
                    }),
                    'rating': rating,
                    'content': comment,
                    'author': author,
                    'publication_date': publication_date.isoformat(),
                    'scrape_date': scrape_date
                }
                reviews_list.append(review_dict)

            if stop_scraping:
                logger.info("Fin : les avis restants datent de plus de 7 jours.")
                break

            next_page_tag = soup.find('a', attrs={"aria-label": "Page suivante"})
            current_url = BASE_URL + next_page_tag['href'] if next_page_tag else None
            time.sleep(random.uniform(2, 5))  # ⚠️ Risque sur CF si trop long

        logger.info(f"{len(reviews_list)} avis collectés.")

        if mode == "json":
            return reviews_list
        elif mode == "pandas":
            return pd.DataFrame(reviews_list)
        else:  # mode == 'csv'
            output_path = "/tmp/avis_boutique.csv"
            with open(output_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=[
                    'review_id', 'rating', 'content', 'author',
                    'publication_date', 'scrape_date'
                ], quoting=csv.QUOTE_ALL)
                writer.writeheader()
                for review in reviews_list:
                    writer.writerow(review)
            logger.info(f"Fichier sauvegardé dans {output_path}")

    except Exception as e:
        logger.error(f"Erreur durant le scraping : {e}", exc_info=True)
        raise

# Exécution directe possible (facilite les tests en local)
if __name__ == "__main__":
    scrape_reviews(mode="csv")

import datetime
from decimal import Decimal
import unicodedata
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os


# Por c item recorrer y popular dataframe
# Volcar todo a un file
csv_location = "input.csv"
base_url = ""
usdQuotation = 1150
kmsScoringMultiplier = 200000

class BusquedaModelo:
    def __init__(self, url, transmision, tipoDePublicacion, marca, modelo, version, carroceria):
        self.Url = url #e.g.: https://autos.mercadolibre.com.ar/fiat/500/manual/fiat-500-sport
        self.Transmision = transmision
        self.TipoDePublicacion = tipoDePublicacion
        self.Marca = marca
        self.Modelo = modelo
        self.Version = version
        self.Carroceria = carroceria

    def __str__(self):
        return 'url:{}, transmision:{}, tipoDePublicacion:{}, marca:{}, modelo:{}, version:{}, carroceria:{}'.format(
            self.Url,
            self.Transmision,
            self.TipoDePublicacion,
            self.Marca,
            self.Modelo,
            self.Version,
            self.Carroceria
        )

class CsvParser():

    def readFile(self, file_location):
        df = pd.read_csv(file_location)
        return list(map(lambda x:BusquedaModelo(
            url=x[0],
            transmision=x[1],
            tipoDePublicacion=x[2],
            marca=x[3],
            modelo=x[4],
            version=x[5],
            carroceria=x[6]
            ),df.values.tolist()))

    

class Scraper():

    parsed_results = []
    
    def process_post(self, post, scrapingInput:BusquedaModelo):
        # get the title
        title = post.find('h2').text
        
        # CUSTOM
        currency = post.find('span', class_='andes-money-amount__currency-symbol').text

        attributes = post.find_all('li', class_='ui-search-card-attributes__attribute')
        year = int(attributes[0].text)
        kms = float(attributes[1].text.replace(".","").replace(" Km", ""))

        city = post.find('span', class_='ui-search-item__group__element ui-search-item__location').text

        score = round(year + kms / kmsScoringMultiplier, 3)

        # get the price
        if(currency == "U$S"):
            price = Decimal(post.find('span', class_='andes-money-amount__fraction').text) * 1000
            usd_price = price
        else:
            price = Decimal(post.find('span', class_='andes-money-amount__fraction').text.replace(".",""))
            usd_price = int((price / usdQuotation))

        # get the url post
        post_link = post.find("a")["href"]
        # get the url image
        try:
            img_link = post.find("img")["data-src"]
        except:
            img_link = post.find("img")["src"]

        scraping_date = datetime.datetime.now().strftime("%Y-%m-%d")

        # save in a dictionary
        post_data = {
            "title": title,
            "marca": scrapingInput.Marca,
            "modelo": scrapingInput.Modelo,
            "version": scrapingInput.Version,
            "carroceria": scrapingInput.Carroceria,
            "tipoDePublicacion": scrapingInput.TipoDePublicacion,
            "transmision": scrapingInput.Transmision,
            "price": price,
            "post link": post_link,
            "image link": img_link,
            "currency": currency,
            "year": year,
            "kms": kms,
            "city": city,
            "usd price": usd_price,
            "score": score,
            "scraping_date": scraping_date         
        }

        if post_data.year > 2024:
            return

        # save the dictionaries in a list
        self.parsed_results.append(post_data)

    def scraping(self, scrapingInputs):
        #for each scraping input, generate n urls + dump data into Scraper property
        for scrapingInput in scrapingInputs:
            #initialize url with base url
            urls = list([scrapingInput.Url])
            print(urls)

            #create other pages urls
            page_number = 50
            for i in range(0, 10000, 50):
                urls.append(f"{scrapingInput.Url}_Desde_{page_number + 1}_NoIndex_True")
                page_number += 50
        
            # Iterate over each url
            for i, url in enumerate(urls, start=1):

                # Get the html of the first page
                response = requests.get(url)

                if i == 1:
                    filenameUrl = url.replace("/", "_").replace(":","").replace(".","")
                    f = open(r"data/html/" + filenameUrl + ".html", "w", encoding="utf-8")
                    f.write(response.text)
                    f.close()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                    
                # take all posts
                content = soup.find_all('li', class_='ui-search-layout__item')
                
                # Check if there's no content to scrape
                if not content:
                    print("\nTermino el scraping.")
                    break

                print(f"\nScrapeando pagina numero {i}. {url}")
                                    
                # iteration to scrape posts
                for post in content:
                    self.process_post(post, scrapingInput)

    
    def export_to_csv(self, filename):
        # export to a csv file
        df = pd.DataFrame(self.parsed_results)
        filename = filename.replace("/", "_").replace(":","").replace(".","")
        time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        df.to_csv(r"data/" + filename + time + ".csv", sep=";")

if __name__ == "__main__":
    csv_parser = CsvParser()
    scrapingInput = csv_parser.readFile(csv_location)

    s = Scraper()
    s.scraping(scrapingInputs=scrapingInput)

    s.export_to_csv("pruebas 3-3-2024")
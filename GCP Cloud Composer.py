from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging
import re
from datetime import datetime
from mysql.connector import Error

# BigQuery Config
GCP_PROJECT = "dynamic-branch-441814-f1"
BQ_DATASET = "dynamic-branch-441814-f1.MI3390Project2"
BQ_PRODUCT = "dynamic-branch-441814-f1.MI3390Project2.product"
BQ_CATEGORY = "dynamic-branch-441814-f1.MI3390Project2.category"
BQ_RATINGS = "dynamic-branch-441814-f1.MI3390Project2.ratings"
BQ_SELLER_RANKING = "dynamic-branch-441814-f1.MI3390Project2.seller_ranking"

continent_dict = {
    'Afghanistan': 'Asia',
    'Albania': 'Europe',
    'Algeria': 'Africa',
    'Andorra': 'Europe',
    'Angola': 'Africa',
    'Antigua and Barbuda': 'North America',
    'Argentina': 'South America',
    'Armenia': 'Asia',
    'Australia': 'Oceania',
    'Austria': 'Europe',
    'Azerbaijan': 'Asia',
    'Bahamas': 'North America',
    'Bahrain': 'Asia',
    'Bangladesh': 'Asia',
    'Barbados': 'North America',
    'Belarus': 'Europe',
    'Belgium': 'Europe',
    'Belize': 'North America',
    'Benin': 'Africa',
    'Bhutan': 'Asia',
    'Bolivia': 'South America',
    'Bosnia and Herzegovina': 'Europe',
    'Botswana': 'Africa',
    'Brazil': 'South America',
    'Brunei': 'Asia',
    'Bulgaria': 'Europe',
    'Burkina Faso': 'Africa',
    'Burundi': 'Africa',
    'Cabo Verde': 'Africa',
    'Cambodia': 'Asia',
    'Cameroon': 'Africa',
    'Canada': 'North America',
    'Central African Republic': 'Africa',
    'Chad': 'Africa',
    'Chile': 'South America',
    'China': 'Asia',
    'Colombia': 'South America',
    'Comoros': 'Africa',
    'Congo, Democratic Republic of the': 'Africa',
    'Congo, Republic of the': 'Africa',
    'Costa Rica': 'North America',
    'Croatia': 'Europe',
    'Cuba': 'North America',
    'Cyprus': 'Asia',
    'Czech Republic': 'Europe',
    'Denmark': 'Europe',
    'Djibouti': 'Africa',
    'Dominica': 'North America',
    'Dominican Republic': 'North America',
    'Ecuador': 'South America',
    'Egypt': 'Africa',
    'El Salvador': 'North America',
    'Equatorial Guinea': 'Africa',
    'Eritrea': 'Africa',
    'Estonia': 'Europe',
    'Eswatini': 'Africa',
    'Ethiopia': 'Africa',
    'Fiji': 'Oceania',
    'Finland': 'Europe',
    'France': 'Europe',
    'Gabon': 'Africa',
    'Gambia': 'Africa',
    'Georgia': 'Asia',
    'Germany': 'Europe',
    'Ghana': 'Africa',
    'Greece': 'Europe',
    'Grenada': 'North America',
    'Guatemala': 'North America',
    'Guinea': 'Africa',
    'Guinea-Bissau': 'Africa',
    'Guyana': 'South America',
    'Haiti': 'North America',
    'Honduras': 'North America',
    'Hungary': 'Europe',
    'Iceland': 'Europe',
    'India': 'Asia',
    'Indonesia': 'Asia',
    'Iran': 'Asia',
    'Iraq': 'Asia',
    'Ireland': 'Europe',
    'Israel': 'Asia',
    'Italy': 'Europe',
    'Jamaica': 'North America',
    'Japan': 'Asia',
    'Jordan': 'Asia',
    'Kazakhstan': 'Asia',
    'Kenya': 'Africa',
    'Kiribati': 'Oceania',
    'Korea, North': 'Asia',
    'Korea, South': 'Asia',
    'Kosovo': 'Europe',
    'Kuwait': 'Asia',
    'Kyrgyzstan': 'Asia',
    'Laos': 'Asia',
    'Latvia': 'Europe',
    'Lebanon': 'Asia',
    'Lesotho': 'Africa',
    'Liberia': 'Africa',
    'Libya': 'Africa',
    'Liechtenstein': 'Europe',
    'Lithuania': 'Europe',
    'Luxembourg': 'Europe',
    'Madagascar': 'Africa',
    'Malawi': 'Africa',
    'Malaysia': 'Asia',
    'Maldives': 'Asia',
    'Mali': 'Africa',
    'Malta': 'Europe',
    'Marshall Islands': 'Oceania',
    'Mauritania': 'Africa',
    'Mauritius': 'Africa',
    'Mexico': 'North America',
    'Micronesia': 'Oceania',
    'Moldova': 'Europe',
    'Monaco': 'Europe',
    'Mongolia': 'Asia',
    'Montenegro': 'Europe',
    'Morocco': 'Africa',
    'Mozambique': 'Africa',
    'Myanmar': 'Asia',
    'Namibia': 'Africa',
    'Nauru': 'Oceania',
    'Nepal': 'Asia',
    'Netherlands': 'Europe',
    'New Zealand': 'Oceania',
    'Nicaragua': 'North America',
    'Niger': 'Africa',
    'Nigeria': 'Africa',
    'North Macedonia': 'Europe',
    'Norway': 'Europe',
    'Oman': 'Asia',
    'Pakistan': 'Asia',
    'Palau': 'Oceania',
    'Palestine': 'Asia',
    'Panama': 'North America',
    'Papua New Guinea': 'Oceania',
    'Paraguay': 'South America',
    'Peru': 'South America',
    'Philippines': 'Asia',
    'Poland': 'Europe',
    'Portugal': 'Europe',
    'Qatar': 'Asia',
    'Romania': 'Europe',
    'Russia': 'Europe',
    'Rwanda': 'Africa',
    'Saint Kitts and Nevis': 'North America',
    'Saint Lucia': 'North America',
    'Saint Vincent and the Grenadines': 'North America',
    'Samoa': 'Oceania',
    'San Marino': 'Europe',
    'Sao Tome and Principe': 'Africa',
    'Saudi Arabia': 'Asia',
    'Senegal': 'Africa',
    'Serbia': 'Europe',
    'Seychelles': 'Africa',
    'Sierra Leone': 'Africa',
    'Singapore': 'Asia',
    'Slovakia': 'Europe',
    'Slovenia': 'Europe',
    'Solomon Islands': 'Oceania',
    'Somalia': 'Africa',
    'South Africa': 'Africa',
    'South Sudan': 'Africa',
    'Spain': 'Europe',
    'Sri Lanka': 'Asia',
    'Sudan': 'Africa',
    'Suriname': 'South America',
    'Sweden': 'Europe',
    'Switzerland': 'Europe',
    'Syria': 'Asia',
    'Taiwan': 'Asia',
    'Tajikistan': 'Asia',
    'Tanzania': 'Africa',
    'Thailand': 'Asia',
    'Timor-Leste': 'Asia',
    'Togo': 'Africa',
    'Tonga': 'Oceania',
    'Trinidad and Tobago': 'North America',
    'Tunisia': 'Africa',
    'Turkey': 'Asia',
    'Turkmenistan': 'Asia',
    'Tuvalu': 'Oceania',
    'Uganda': 'Africa',
    'Ukraine': 'Europe',
    'United Arab Emirates': 'Asia',
    'United Kingdom': 'Europe',
    'United States': 'North America',
    'Uruguay': 'South America',
    'Uzbekistan': 'Asia',
    'Vanuatu': 'Oceania',
    'Vatican City': 'Europe',
    'Venezuela': 'South America',
    'Vietnam': 'Asia',
    'Yemen': 'Asia',
    'Zambia': 'Africa',
    'Zimbabwe': 'Africa'
}

mc_dict = {
    'Art & Crafts': 'ART001',
    'Computer': 'COM001',
    'Electronics': 'ELE001'
}

sc_dict = {
    'Accessories & Peripherals': 'ACC001',
    'Accessories & Supplies': 'ACC002',
    'Beading & Jewelry Making': 'BEA001',
    'Camera & Photo': 'CAM001',
    'Car & Vehicle Electronics': 'CAR001',
    'Computer Accessories & Peripherals': 'COM001',
    'Computers & Accessories': 'COM002',
    'Computers & Tablets': 'COM003',
    'Computer Components': 'COM004',
    'Cell Phones & Accessories': 'CEL001',    
    'Crafting': 'CRA001',
    'Data Storage': 'DAT001',
    'eBook Readers & Accessories': 'EBO001',
    'External Components': 'EXT001',
    'Fabric': 'FAB001',
    'Fabric Decorating': 'FAB002',
    'Gift Wrapping Supplies': 'GIF001',
    'GPS & Navigation': 'GPS001',
    'Headphones': 'HEA001',
    'Home Audio': 'HOM001',
    'Knitting & Crochet': 'KNI001',
    'Laptop Accessories': 'LAP001',
    'Monitors': 'MON001',
    'Needlework': 'NED001',
    'Networking Products': 'NET001',
    'Office Electronics': 'OFF001',
    'Organization, Storage & Transport': 'ORG001',
    'Painting, Drawing & Art Supplies': 'PAI001',
    'Party Decorations & Supplies': 'PAR001',
    'Portable Audio & Video': 'POR001',
    'Printmaking': 'PRI001',
    'Printers': 'PRI002',
    'Scanners': 'SCA001',
    'Servers': 'SER001',
    'Sewing': 'SEW001',
    'Scrapbooking & Stamping': 'SCR001',
    'Security & Surveillance': 'SEC001',
    'Tablet Accessories': 'TAB001',
    'Tablet Replacement Parts': 'TAB002',
    'Video Game Consoles & Accessories': 'VID001',
    'Video Projectors': 'VID002',
    'Warranties & Services': 'WAR001',
    'Wearable Technology': 'WEA001'
}

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.183 Safari/537.36"
]
    
def get_product_details(url):
    print(f"Đang lấy thông tin sản phẩm từ: {url}")
    
    headers = {
        "User-Agent": random.choice(user_agents),  # Giả mạo User-Agent
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",  # Loại dữ liệu muốn nhận
        "Accept-Language": "en-US,en;q=0.9",  # Ngôn ngữ ưu tiên
        "Accept-Encoding": "gzip, deflate, br",  # Kiểu mã hóa nén được hỗ trợ
        "Referer": "https://www.google.com/",  # URL nguồn, ví dụ: từ Google
        "Connection": "keep-alive",  # Giữ kết nối mở
        "Upgrade-Insecure-Requests": "1",  # Yêu cầu chuyển đổi HTTP sang HTTPS (thường thấy trong các trình duyệt hiện đại)
        "Sec-Fetch-Dest": "document",  # Đích đến của yêu cầu (tài liệu HTML)
        "Sec-Fetch-Mode": "navigate",  # Phương thức điều hướng
        "Sec-Fetch-Site": "same-origin",  # Nguồn gốc của yêu cầu
        "Sec-Fetch-User": "?1",  # Chỉ báo người dùng (có hay không)
        "Cache-Control": "max-age=0",  # Điều khiển bộ nhớ đệm
    }

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    details = {
        'title': None,
        'price_before_discount': None,
        'discount_percentage': None,
        'overall_rating': None,
        'review_count': None,
        'description': None,
        'UPC': None,
        'ASIN': None,
        'Manufacturer': None,
        'Brand Name': None,
        'Best Sellers Rank': None,
        'Country of Origin': None,
        'Price': None,
        'AmazonGlobal Shipping': None,
        'Estimated Import Charges': None,
        'Total': None,
        'Color': None,
        '5 star': None,  
        '4 star': None,  
        '3 star': None,  
        '2 star': None,  
        '1 star': None, 
        'Item Weight': None,
        'Product Dimensions': None,
        'Volumn': None, 
        'units_sold_last_month': None,  
        'delivery_time_days': None,
        'Category': Category,  # Thêm Category
        'Subcategory': Subcategory  # Thêm Subcategory
    }
    
    try:
        details['title'] = soup.find('span', {'id': 'productTitle'}).get_text().strip()
    except AttributeError:
        pass
    
    try:
        details['price_before_discount'] = soup.find('span', {'class': 'a-price a-text-price', 'data-a-size': 's'}).find('span', {'class': 'a-offscreen'}).get_text().strip().replace('$', '')
    except AttributeError:
        pass
    
    try:
        details['discount_percentage'] = soup.find('span', {'class': 'a-size-large a-color-price savingPriceOverride aok-align-center reinventPriceSavingsPercentageMargin savingsPercentage'}).get_text().strip().replace('-', '')
    except AttributeError:
        pass
    
    try:
        details['overall_rating'] = soup.find('span', {'class': 'a-icon-alt'}).get_text().strip()
    except AttributeError:
        pass
    
    try:
        details['review_count'] = soup.find('span', {'id': 'acrCustomerReviewText'}).get_text().strip()
    except AttributeError:
        pass
    
    try:
        details['description'] = soup.find('div', {'id': 'productDescription'}).get_text().strip()
    except AttributeError:
        pass
    
    # Lấy đánh giá sao và phần trăm
    try:
        stars_element = soup.find('div', class_='a-section a-spacing-none a-text-left aok-nowrap')
        percentage_element = soup.find('div', class_='a-section a-spacing-none a-text-right aok-nowrap')

        if stars_element and percentage_element:
            stars = stars_element.find_all('span', class_='_cr-ratings-histogram_style_histogram-column-space__RKUAd')
            percentages = percentage_element.find_all('span', class_='_cr-ratings-histogram_style_histogram-column-space__RKUAd')

            if len(stars) == len(percentages):
                for star, percentage in zip(stars, percentages):
                    rating = star.get_text(strip=True)
                    percent = percentage.get_text(strip=True)
                    details[rating] = percent
    except AttributeError:
        pass

    # Tìm kiếm Brand Name và Manufacturer bằng từ đồng nghĩa
    synonyms = {
        'Brand Name': ['Brand Name', 'Brand'],
    }

    key_value_pairs = extract_key_value_pairs(soup)
    for key, keys_list in synonyms.items():
        for header in keys_list:
            value = next((value for h, value in key_value_pairs if header.lower() in h.lower()), None)
            if value:
                details[key] = value
                break

    try:
        sold_units_element = soup.find('span', {'id': 'social-proofing-faceout-title-tk_bought'})
        if sold_units_element:
            details['units_sold_last_month'] = sold_units_element.get_text(strip=True)
    except AttributeError:
        pass
    
    try:
        delivery_element = soup.find('span', {'data-csa-c-type': 'element', 'data-csa-c-content-id': 'DEXUnifiedCXPDM'})
        if delivery_element:
            delivery_time = delivery_element.get('data-csa-c-delivery-time')
            if delivery_time:
                delivery_date_str = delivery_time  
                delivery_date = datetime.strptime(delivery_date_str, '%A, %B %d')
                current_date = datetime.now()
                delivery_date = delivery_date.replace(year=current_date.year)
                days_left = (delivery_date - current_date).days
                details['delivery_time_days'] = days_left if days_left >= 0 else 0
    except (AttributeError, ValueError) as e:
        print(f'Error occurred: {e}')

    # Cào thông tin từ các bảng
    tables = soup.find_all('table', class_='a-keyvalue prodDetTable')
    for table in tables:
        for row in table.find_all('tr'):
            header = row.find('th').text.strip()
            value = row.find('td').text.strip()
            if header in details:
                details[header] = value

    tables = soup.find_all('table', class_='a-lineitem')
    for table in tables:
        for row in table.find_all('tr'):
            header_td = row.find('td', class_='a-span9 a-text-left')           
            value_td = row.find('td', class_='a-span2 a-text-right')
            
            if header_td and value_td:
                header_span = header_td.find('span')
                value_span = value_td.find('span')
                
                if header_span and value_span:
                    header = header_span.text.strip()
                    value = value_span.text.strip()
                    if header in details:
                        details[header] = value
                
    tables = soup.find_all('table', class_='a-normal a-spacing-micro')
    for table in tables:
        for row in table.find_all('tr'):
            header = row.find('td', class_='a-span3').text.strip()
            value = row.find('td', class_='a-span9').text.strip()
            if header == 'Color':
                details['Color'] = value
                break
    return details

def extract_key_value_pairs(soup):
    """Hàm để lấy các cặp khóa-giá trị từ các bảng sản phẩm"""
    key_value_pairs = []
    tables = soup.find_all('table', class_='a-keyvalue prodDetTable')
    for table in tables:
        for row in table.find_all('tr'):
            header = row.find('th').text.strip()
            value = row.find('td').text.strip()
            key_value_pairs.append((header, value))
    return key_value_pairs

def get_products_from_subcategory(subcategory_url, pages_to_scrape):
    print(f"Đang lấy danh sách sản phẩm từ subcategory: {subcategory_url}")
        
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",  # Giả mạo User-Agent
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",  # Loại dữ liệu muốn nhận
        "Accept-Language": "en-US,en;q=0.9",  # Ngôn ngữ ưu tiên
        "Accept-Encoding": "gzip, deflate, br",  # Kiểu mã hóa nén được hỗ trợ
        "Referer": "https://www.google.com/",  # URL nguồn, ví dụ: từ Google
        "Connection": "keep-alive",  # Giữ kết nối mở
        "Upgrade-Insecure-Requests": "1",  # Yêu cầu chuyển đổi HTTP sang HTTPS (thường thấy trong các trình duyệt hiện đại)
        "Sec-Fetch-Dest": "document",  # Đích đến của yêu cầu (tài liệu HTML)
        "Sec-Fetch-Mode": "navigate",  # Phương thức điều hướng
        "Sec-Fetch-Site": "same-origin",  # Nguồn gốc của yêu cầu
        "Sec-Fetch-User": "?1",  # Chỉ báo người dùng (có hay không)
        "Cache-Control": "max-age=0",  # Điều khiển bộ nhớ đệm
    }

    product_links = []
    for page in range(1, pages_to_scrape + 1):
        response = requests.get(f"{subcategory_url}&page={page}", headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        for link in soup.find_all('a', {'class': 'a-link-normal s-no-outline'}):
            product_links.append('https://www.amazon.com' + link['href'])
        
        print(f"Tìm thấy {len(product_links)} sản phẩm ở trang {page}.")
    
    return product_links

def scrape_amazon_data(category, subcategory, subcategory_url, pages_to_scrape):
    print(f"Bắt đầu cào dữ liệu cho category: {category}, subcategory: {subcategory}")
    product_links = get_products_from_subcategory(subcategory_url, pages_to_scrape)
    product_data = []
    
    for index, link in enumerate(product_links):
        print(f"Đang xử lý sản phẩm {index + 1}/{len(product_links)}")
        details = get_product_details(link)
        details['Category'] = category
        details['Subcategory'] = subcategory
        product_data.append(details)
        time.sleep(random.uniform(1, 3))  # Thêm thời gian nghỉ giữa các lần cào để tránh bị chặn
    
    if not product_data:
        print("Không có dữ liệu sản phẩm để lưu.")
        return
    
    df = pd.DataFrame(product_data)
    return df.to_json()

def transform_data(details):
    try:
        """Hàm thực hiện các yêu cầu xử lý dữ liệu."""
        
        # 2.1. Bỏ phần "out of 5 stars" khỏi overall_rating
        if details['overall_rating']:
            details['overall_rating'] = details['overall_rating'].split(' out of ')[0]

        # 2.2. Bỏ phần "ratings" khỏi review_count và chuyển đổi thành số nguyên
        if details['review_count']:
            # Loại bỏ từ "ratings" và các khoảng trắng
            review_count_str = re.sub(r'\s*ratings$', '', details['review_count']).strip()  # Bỏ "ratings" ở cuối và xóa khoảng trắng

            # Bỏ dấu phẩy (nếu có)
            review_count_str = review_count_str.replace(',', '')  # Bỏ dấu phẩy

            # Chuyển đổi sang số nguyên, nếu không thể thì gán là None
            try:
                details['review_count'] = int(review_count_str)  # Chuyển đổi sang số nguyên
            except ValueError:
                details['review_count'] = None  # Gán là None nếu không thể chuyển đổi

        # 2.3. UPC xử lý
        if details['UPC']:
            upc_list = details['UPC'].split(' ')  # Giả sử nhiều UPC cách nhau bằng dấu cách
            if upc_list:  # Kiểm tra xem upc_list không rỗng
                upc_first = upc_list[0].strip()  # Lấy UPC đầu tiên và bỏ khoảng trắng
                if upc_first.isdigit():  # Kiểm tra xem giá trị có phải là số không
                    details['UPC'] = upc_first  # Giữ lại UPC đầu tiên
                else:
                    details['UPC'] = None  # Nếu không hợp lệ, đặt thành None
            else:
                details['UPC'] = None  # Nếu upc_list rỗng, đặt thành None
        else:
            details['UPC'] = None  # Nếu không có UPC, đặt thành None

        # 2.4. Tách Best Sellers Rank
        if details['Best Sellers Rank']:
            # Find rank numbers and categories
            ranks = re.findall(r'#([\d,]+)', details['Best Sellers Rank'])
            categories = re.findall(r'in ([^#]+)', details['Best Sellers Rank'])
            
            # Parse and remove commas from ranks, convert to integer
            ranks = [int(rank.replace(',', '')) for rank in ranks]
            
            # Remove content in parentheses from categories
            categories = [re.sub(r'\s*\(.*?\)', '', category).strip() for category in categories]
            
            # Initialize fields
            details['mainrankingvalue'] = None
            details['mainrankingcategory'] = None
            details['subrankingvalue'] = None
            details['subrankingcategory'] = None
            
            if len(ranks) >= 2:
                # Assign the first rank and category to mainranking
                details['mainrankingvalue'] = ranks[0]
                details['mainrankingcategory'] = categories[0] if len(categories) > 0 else None
                
                # Assign the second rank and category to subranking
                details['subrankingvalue'] = ranks[1]
                details['subrankingcategory'] = categories[1] if len(categories) > 1 else None
            elif len(ranks) == 1:
                # If only one rank, assign it to subranking, leave mainranking empty
                details['subrankingvalue'] = ranks[0]
                details['subrankingcategory'] = categories[0] if categories else None
        
        country = details.get('Country of Origin', None)
        details['Country of Origin'] = country
        details['Continent'] = continent_dict.get(country, 'Unknown')
        
        # 2.5. Bỏ dấu $ trong các trường giá
        for field in ['Price', 'AmazonGlobal Shipping', 'Estimated Import Charges', 'Total']:
            if details.get(field):
                if '$' in details[field]:
                    details[field] = details[field].replace('$', '')
                    if ',' in details[field]:
                        details[field] = details[field].replace(',', '') 

        # 2.6. Bỏ dấu % và chuyển đổi về dạng số cho các sao
        for star in ['discount_percentage', '5 star', '4 star', '3 star', '2 star', '1 star']:
            if details.get(star):
                details[star] = float(details[star].replace('%', '')) / 100

        # 2.7. Chuyển Item Weight về pounds
        if details['Item Weight']:
            weight = details['Item Weight'].strip()
            
            # Khởi tạo biến pounds
            pounds = None

            try:
                # Chỉ giữ lại các ký tự số và dấu chấm
                weight = re.sub(r'[^0-9.]', '', weight)

                if 'ounces' in details['Item Weight']:
                    # Chuyển ounces sang pounds
                    pounds = float(weight) / 16  # Chuyển ounces sang pounds
                elif 'pounds' in details['Item Weight']:
                    # Chuyển đổi giá trị pounds
                    pounds = float(weight)
            except ValueError:
                print(f"Error converting weight: {weight}")  # In ra lỗi nếu không thể chuyển đổi

            details['Item Weight'] = pounds  # Gán giá trị pounds vào trường Item Weight
        
        # 2.8. Chuyển Product Dimensions về dạng số
        if details['Product Dimensions']:
            dimensions = details['Product Dimensions'].strip()
        
            # Bỏ các ký tự như inches, ", L, W, H, D và bất kỳ khoảng trắng dư thừa
            dimensions = re.sub(r'(\binches\b|["LWDH])', '', dimensions)
        
            # Tách các giá trị dựa trên 'x' hoặc các từ khóa để tạo danh sách số
            if 'x' in dimensions:
                dimensions_list = re.split(r'\s*x\s*', dimensions.lower())
            else:
                dimensions_list = re.split(r'\s+', dimensions.lower())

            # Chuyển tất cả giá trị thành float, bỏ qua giá trị không phải số
            try:
                details['Product Dimensions'] = [float(dim.strip()) for dim in dimensions_list if dim.strip().replace('.', '', 1).isdigit()]
            except ValueError:
                details['Product Dimensions'] = None  # Nếu không thể chuyển đổi, đặt giá trị là None

        # Kiểm tra trước khi lặp qua Product Dimensions
        if details['Product Dimensions'] is not None:
            details['Volumn'] = 1  # Khởi tạo giá trị nếu cần
            for dimension in details['Product Dimensions']:
                details['Volumn'] *= dimension
        else:
            details['Volumn'] = None  # Hoặc gán giá trị mặc định khác nếu cần
            
        # 2.9. Xử lý units_sold_last_month
        if details['units_sold_last_month']:
            sold_text = str(details['units_sold_last_month']).replace(' bought in past month', '')

            if '+' in sold_text:
                sold_text = sold_text.split('+')[0]  # Bỏ phần sau dấu +

            if 'K' in sold_text:
                sold_text = sold_text.replace('K', '000')  # Thêm 3 số không nếu có K
        
            # Chuyển đổi thành số nguyên
            try:
                details['units_sold_last_month'] = int(float(sold_text))
            except ValueError:
                details['units_sold_last_month'] = None  # Đặt None nếu không thể chuyển đổi

        # 2.10. Tạo CategoryID
        # Thêm phần tạo CategoryID
        category = details.get('Category')
        subcategory = details.get('Subcategory')
        
        # Lấy CategoryID từ mc_dict và sc_dict
        mc_value = mc_dict.get(category)
        sc_value = sc_dict.get(subcategory)
        
        if mc_value and sc_value:
            details['CategoryID'] = f"{mc_value}{sc_value}"  # Ghép lại thành CategoryID
        else:
            details['CategoryID'] = None  # Nếu không tìm thấy giá trị nào
            
    except Exception as e:
        time.sleep(10)
        logging.error(f"Error transforming data: {e}, Details: {details}")
        # Trả về giá trị mặc định nếu gặp lỗi
        details = {key: None for key in details.keys()}
        
        
    return details

def process(details):
    """Chuyển đổi chuỗi JSON về DataFrame và áp dụng hàm transform_data."""
    details = pd.read_json(details)  # Chuyển từ JSON sang DataFrame
    transformed_details = details.apply(transform_data, axis = 1)
    return transformed_details.to_json()

def load_data(details_json):
    details = pd.read_json(details_json)
    try:
        # Khởi tạo BigQuery Client
        client = bigquery.Client(project=GCP_PROJECT)
        
        # Cấu hình công việc tải dữ liệu
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # Ghi thêm dữ liệu vào bảng
        )
        
        for _, row in details.iterrows():
            # Format Product Dimensions if it's a list
            if isinstance(row['Product Dimensions'], list):
                row['Product Dimensions'] = ' x '.join(map(str, row['Product Dimensions']))

            # Convert empty or NaN values to None for SQL NULL
            row = {key: (None if pd.isnull(value) else value) for key, value in row.items()}

            # Tạo table_id hoàn chỉnh
            full_table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_PRODUCT}"
        
            # Gửi DataFrame vào BigQuery
            load_job = client.load_table_from_dataframe(
                row, full_table_id, job_config=job_config
            )

            # Chờ hoàn thành
            load_job.result()
                        
            # Tạo table_id hoàn chỉnh
            full_table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_CATEGORY}"
        
            # Gửi DataFrame vào BigQuery
            load_job = client.load_table_from_dataframe(
                row, full_table_id, job_config=job_config
            )

            # Chờ hoàn thành
            load_job.result()
                        
            # Tạo table_id hoàn chỉnh
            full_table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_RATINGS}"
        
            # Gửi DataFrame vào BigQuery
            load_job = client.load_table_from_dataframe(
                row, full_table_id, job_config=job_config
            )

            # Chờ hoàn thành
            load_job.result()  
                      
            # Tạo table_id hoàn chỉnh
            full_table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_RATINGS}"
        
            # Gửi DataFrame vào BigQuery
            load_job = client.load_table_from_dataframe(
                row, full_table_id, job_config=job_config
            )
                        
            # Chờ hoàn thành
            load_job.result()
            
    except Error:
        time.sleep(10)
        pass

# Define default args for the DAG
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'amazon_etl_gcp',
    default_args=default_args,
    description='ETL pipeline for Amazon product data on GCP',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 26),
    catchup=False,
)

Category = 'Art & Crafts'
Subcategory = 'Painting, Drawing & Art Supplies'
subcategory_url = 'https://www.amazon.com/s?i=specialty-aps&bbn=4954955011&rh=n%3A4954955011%2Cn%3A%212617942011%2Cn%3A2747968011&ref=nav_em__nav_desktop_sa_intl_painting_drawing_supplies_0_2_8_2'
pages_to_scrape = 1

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=scrape_amazon_data,
    op_args=[Category, Subcategory, subcategory_url, pages_to_scrape],
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=process,
    op_args=['{{ ti.xcom_pull(task_ids="extract_task") }}'],
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    op_args=['{{ ti.xcom_pull(task_ids="transform_task") }}'],
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
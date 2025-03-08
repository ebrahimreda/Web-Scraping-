import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime

# -----------------------------
# 1) إعداد عناوين وروابط
# -----------------------------
url_csv = 'web/jobes.csv'
url = "https://medrecruit.medworld.com/jobs/list?location=New+South+Wales&page=1&locationName=New+South+Wales"


# -----------------------------
# 2) دوال استخراج البيانات من الصفحة (Scraping)
# -----------------------------
def extract_job_title(card):
    title_elem = card.find("h2", {"data-testid": "job-card-speciality"})
    return title_elem.text.strip() if title_elem else "N/A"

def extract_department(card):
    dept_elem = card.find("p", {"data-testid": "job-card-grade"})
    return dept_elem.text.strip() if dept_elem else "N/A"

def extract_location(card):
    city_elem = card.find("span", {"data-testid": "job-location-city"})
    state_elem = card.find("span", {"data-testid": "job-location-state"})
    country_elem = card.find("span", {"data-testid": "job-location-country"})
    location_parts = [elem.text.strip() for elem in (city_elem, state_elem, country_elem) if elem]
    # سيُحفظ مثلاً بشكل "Sydney NSW Australia"
    return " ".join(location_parts) if location_parts else "N/A"

def extract_job_type(card):
    type_elem = card.find("div", {"data-testid": "Work Type"})
    return type_elem.text.strip() if type_elem else "N/A"

def extract_salary(card):
    salary_elem = card.find("div", {"data-testid": "Salary"})
    return salary_elem.text.strip() if salary_elem else "N/A"

def extract_date(card):
    date_elem = card.find("div", {"data-testid": "Date"})
    return date_elem.text.strip() if date_elem else "N/A"

def extract_url(card):
    link_elem = card.find("a", {"class": "JobCard_title__jdBTC"})
    return link_elem["href"].strip() if link_elem and link_elem.has_attr("href") else "N/A"


# -----------------------------
# 3) دوال تنظيف البيانات
# -----------------------------
def parse_dates(date_str):
    """
    مثال: "19 Sep 2025 – 10 Oct 2025" 
    سيتم استخراج التاريخين وحساب عدد الأيام.
    إذا لم يُعثر على نطاق تاريخي صحيح، نُعيد None.
    """
    # نسمح بوجود شرطة قصيرة أو طويلة بين التاريخين
    pattern = r'(\d{1,2} \w{3} \d{4})'
    matches = re.findall(pattern, date_str)
    if len(matches) == 2:
        start_str, end_str = matches
        try:
            start_date = datetime.strptime(start_str, '%d %b %Y')
            end_date = datetime.strptime(end_str, '%d %b %Y')
            return (end_date - start_date).days
        except ValueError:
            return None
    return None

def split_location(location_str):
    """
    سيُفترض أن الموقع محفوظ بشكل "City State Country" أو "N/A".
    سنحاول تقسيمها إلى Suburb, State, Country.
    إذا أردت Postcode، يجب أن يكون موجودًا صراحةً في النص أو تُجري بحثًا إضافيًا.
    """
    if location_str == "N/A":
        return ("N/A", "N/A", "N/A")
    
    parts = location_str.split()
    if len(parts) == 3:
        suburb, state, country = parts
        return (suburb, state, country)
    elif len(parts) == 2:
        suburb, state = parts
        return (suburb, state, "N/A")
    elif len(parts) == 1:
        return (parts[0], "N/A", "N/A")
    else:
        # في حال كان هناك أكثر من 3 أجزاء مثلاً
        # يمكن إعادة تركيبها أو تركها حسب الحاجة
        return (parts[0], " ".join(parts[1:-1]), parts[-1])


# -----------------------------
# 4) الدالة الرئيسية للسكريبت
# -----------------------------
def main():
    # 4.1) جلب المحتوى من الصفحة
    response = requests.get(url)
    html_content = response.content
    
    # 4.2) تحليل الصفحة (BeautifulSoup)
    soup = BeautifulSoup(html_content, "html.parser")
    job_cards = soup.find_all("article", {"data-testid": "job-card"})
    
    if not job_cards:
        print("No job cards found.")
        return
    
    # 4.3) استخراج البيانات من كل بطاقة وظيفة
    jobs_data = []
    for card in job_cards:
        job_title = extract_job_title(card)
        department = extract_department(card)
        location = extract_location(card)
        job_type = extract_job_type(card)
        salary = extract_salary(card)
        date_info = extract_date(card)
        url_link = extract_url(card)
        
        job_info = {
            "Job Title": job_title,
            "Department": department,
            "Location": location,
            "Job Type": job_type,
            "Salary": salary,
            "Date": date_info,
            "URL Link": "https://medrecruit.medworld.com"+url_link
        }
        jobs_data.append(job_info)
    
    # 4.4) إنشاء DataFrame من القائمة وحفظه في ملف CSV (البيانات الخام)
    df = pd.DataFrame(jobs_data)
    df.to_csv(url_csv, index=False)
    print("Data saved to CSV (raw).")
    
    # 4.5) تنظيف البيانات وإضافة أعمدة جديدة
    #      - عمود Duration_Days
    #      - تقسيم Location إلى Suburb, State, Country
    df['Duration_Days'] = df['Date'].apply(parse_dates)
    df[['Suburb', 'State', 'Country']] = df['Location'].apply(
        lambda loc: pd.Series(split_location(loc))
    )
    
    # 4.6) إنشاء Pivot Table لحساب متوسط مدة الوظيفة بالأيام حسب (Job Title, Department)
    pivot_duration = df.pivot_table(
        index=['Job Title', 'Department'],
        values='Duration_Days',
        aggfunc='mean'
    ).reset_index()
    
    # 4.7) حفظ البيانات النظيفة وملف الـPivot
    df.to_csv('web/jobes_cleaned.csv', index=False)
    pivot_duration.to_csv('web/jobes_pivot_duration.csv', index=False)
    
    print("Data cleaned and pivot tables created successfully!")
    print(" - Cleaned data: web/jobes_cleaned.csv")
    print(" - Pivot table:  web/jobes_pivot_duration.csv")


# -----------------------------
# 5) تشغيل السكريبت
# -----------------------------
if __name__ == "__main__":
    main()

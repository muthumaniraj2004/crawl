"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 104 / 400
================================================================================
- Group: ThirdTest_pincode_group_11_parts_101_to_110
- Assigned PIN Codes: 48 (Range: 383350 to 384335)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_104.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_104.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_104"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-104] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "383350",
  "383355",
  "383410",
  "383421",
  "383422",
  "383430",
  "383434",
  "383440",
  "383450",
  "383460",
  "383462",
  "384001",
  "384002",
  "384003",
  "384005",
  "384012",
  "384110",
  "384120",
  "384130",
  "384135",
  "384140",
  "384151",
  "384160",
  "384170",
  "384220",
  "384221",
  "384225",
  "384229",
  "384230",
  "384240",
  "384241",
  "384245",
  "384246",
  "384255",
  "384260",
  "384265",
  "384266",
  "384272",
  "384275",
  "384285",
  "384290",
  "384305",
  "384310",
  "384315",
  "384320",
  "384325",
  "384330",
  "384335"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "383350": {
    "pincode": "383350",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Meghraj SO",
      "Banthiwada BO",
      "Behdaj BO",
      "Belyo BO",
      "Bhemapur BO",
      "Iploda BO",
      "Kadvadi BO",
      "Kaliya Kuva BO",
      "Kasana BO",
      "Kolundra BO",
      "Kunol BO",
      "Lalpur BO",
      "Limbodara BO",
      "Moti Mori BO",
      "Moti Moydi BO",
      "Jamgadh BO",
      "Panchal BO",
      "Patelna Dhundha BO",
      "Ramgadhi BO",
      "Rayawada BO",
      "Relyo BO",
      "Shangal BO",
      "Sisodara A BO",
      "Undava BO",
      "Vaghpur BO",
      "Valuna BO",
      "Vaniawada BO",
      "Vasna BO",
      "Zarda BO"
    ]
  },
  "383355": {
    "pincode": "383355",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Shamlaji SO",
      "Devnimori BO",
      "Dhambolia BO",
      "Dhandhasan BO",
      "Dodisara BO",
      "Jab Chitaria BO",
      "Karchha BO",
      "Kherancha BO",
      "Napda BO",
      "Ode BO",
      "Palla BO",
      "Panch Mahudi BO",
      "Sarvoday Kendra BO"
    ]
  },
  "383410": {
    "pincode": "383410",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Badoli SO",
      "Ankala BO",
      "Bhanpur BO",
      "Diyoli BO",
      "Ganthiol BO",
      "Goral BO",
      "Kukadia BO",
      "Lalpur BO",
      "Sherpur BO"
    ]
  },
  "383421": {
    "pincode": "383421",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Chitroda SO",
      "BolundraR BO",
      "Dharapur BO",
      "Mota Kotda BO",
      "Poshina BO",
      "Sabli BO"
    ]
  },
  "383422": {
    "pincode": "383422",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Poshina SO",
      "Ajavas BO",
      "Ambamahuda BO",
      "Chandrana BO",
      "Dantral BO",
      "Delwada BO",
      "Ganer BO",
      "Ganva BO",
      "Kalikankar BO",
      "Kotda BO",
      "Petachhapra BO",
      "Pipliya BO",
      "Salera BO",
      "Sembalia Poshina BO",
      "Tadhivedi BO",
      "Vinchhi BO"
    ]
  },
  "383430": {
    "pincode": "383430",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Idar SO",
      "Idar Town SO",
      "Barvav BO",
      "Bhavangadh BO",
      "Gambhirpura BO",
      "Laloda BO",
      "Lei BO",
      "Limbhoi BO",
      "Mathasur BO",
      "Netramali BO",
      "Panol BO",
      "Pratappura BO",
      "Ratanpur BO",
      "Sabalwad BO",
      "Sapawada BO",
      "Surpur BO",
      "Umedpura BO"
    ]
  },
  "383434": {
    "pincode": "383434",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Verabar SO",
      "Badol BO",
      "Chandap BO",
      "Gajipur BO",
      "Golwada BO",
      "Gulabpura BO",
      "Kamalpur BO",
      "Kava BO",
      "Rampur Fudeda BO",
      "Ravol BO"
    ]
  },
  "383440": {
    "pincode": "383440",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Kadiadra SO",
      "Bhutiya BO",
      "Choriwad BO",
      "Chulla BO",
      "Juna Chamu BO",
      "Kalvan BO",
      "Kathvavdi BO",
      "Pratapgadh Kampa BO"
    ]
  },
  "383450": {
    "pincode": "383450",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Vasai SO",
      "Jumsar BO",
      "Kanpur BO",
      "Kishangadh BO",
      "Malasa BO",
      "Mesan BO",
      "Mudeti BO",
      "Munai BO",
      "Nankhi BO",
      "Nava Revas BO",
      "Siyasan BO",
      "Sunsar BO",
      "Vagheswari BO"
    ]
  },
  "383460": {
    "pincode": "383460",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Vijaynagar SO",
      "Abhapur BO",
      "Androkha BO",
      "Atarsumba BO",
      "Bhankhara BO",
      "Chandvasa BO",
      "Kelava BO",
      "Nalseri BO",
      "Navagam Dhanela BO",
      "Padela BO",
      "Parosada BO",
      "Rajpur BO",
      "Saroli BO",
      "Sarsav BO",
      "Ukhala Dungri BO",
      "Vandhol BO",
      "Vasai BO"
    ]
  },
  "383462": {
    "pincode": "383462",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Sabarkantha Division",
    "offices": [
      "Kodiawada SO",
      "Baleta BO",
      "Biladia BO",
      "Chitariya BO",
      "Chithoda BO",
      "Dadhvav BO",
      "Dantod BO",
      "Gadi BO",
      "Jaleti BO",
      "Limda BO",
      "Pal BO",
      "Vankda BO"
    ]
  },
  "384001": {
    "pincode": "384001",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Mahesana HO",
      "Basana BO",
      "Meghaliasana BO",
      "Savala BO",
      "Kherva BO",
      "Sobhasan BO",
      "Tareti BO",
      "Vadosan BO",
      "Dhamanva BO",
      "Gadha BO",
      "Kansarakui BO",
      "Virta BO",
      "Dela BO",
      "Mitha BO",
      "Nagalpur BO",
      "Piludra BO",
      "Udalpur BO",
      "Gorad BO",
      "Sametra BO",
      "Lakhvad BO",
      "Mahesana Bazar SO",
      "Mahesana JSM SO"
    ]
  },
  "384002": {
    "pincode": "384002",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Mahesana I E SO",
      "Mahesana Railway Colony SO"
    ]
  },
  "384003": {
    "pincode": "384003",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Mahesana Ongc Colony SO"
    ]
  },
  "384005": {
    "pincode": "384005",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "PANCHOT S.O",
      "ALODA B.O",
      "CHHATHIARDA B.O",
      "NUGAR B.O",
      "PALODAR B.O"
    ]
  },
  "384012": {
    "pincode": "384012",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Ganpat Vidyanagar SO"
    ]
  },
  "384110": {
    "pincode": "384110",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Balisana SO",
      "Der BO",
      "Kani BO"
    ]
  },
  "384120": {
    "pincode": "384120",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Bhandu SO",
      "Bamosana BO",
      "Bokarwada BO",
      "Butta Paldi BO",
      "Davada BO",
      "Jetalvasana BO",
      "Moti Dau BO"
    ]
  },
  "384130": {
    "pincode": "384130",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Kahoda SO",
      "Bhunav BO",
      "Lunva BO",
      "Machhava BO",
      "Mahervada BO",
      "Mandali BO"
    ]
  },
  "384135": {
    "pincode": "384135",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Brahmanwada SO",
      "Varvada BO",
      "Vishol BO"
    ]
  },
  "384140": {
    "pincode": "384140",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Kamli SO",
      "Jagnathpura BO"
    ]
  },
  "384151": {
    "pincode": "384151",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Sidhpur Jafri Baug SO",
      "Sidhpur Jam Chakla SO",
      "Sidhpur Market Yard SO",
      "Sidhpur SO",
      "Bilia BO",
      "Chandravati BO",
      "Dethli BO",
      "Gangalasan BO",
      "Ganvada BO",
      "Khali BO",
      "Kholvada BO",
      "Meloj BO",
      "Mudana BO",
      "Nagvasana BO",
      "Nandotri BO",
      "Nedra BO",
      "Samoda BO",
      "Sedrana BO",
      "Tavadia BO",
      "Umru BO",
      "Vadhana BO",
      "Vanasan BO",
      "Varsila BO"
    ]
  },
  "384160": {
    "pincode": "384160",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Unava SO"
    ]
  },
  "384170": {
    "pincode": "384170",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Unjha Kotkuva SO",
      "Unjha SO",
      "Unjha Umiya Mataji SO",
      "Aithor BO",
      "Amudh BO",
      "Bhankhar BO",
      "Dabhi BO",
      "Dasaj BO",
      "Hajipur BO",
      "Karli BO",
      "Maktupur BO",
      "Ranchhodpura BO",
      "Sihi BO",
      "Sunok BO",
      "Tundav BO",
      "Upera BO",
      "Vanagala BO"
    ]
  },
  "384220": {
    "pincode": "384220",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Chanasma SO",
      "Brahmanwada BO",
      "Dhanodharda BO",
      "Dharmoda BO",
      "Jakhana BO",
      "Jitoda BO",
      "Khorsam BO",
      "Mithi Vavdi BO",
      "Ruppur BO",
      "Sevala BO",
      "Khari Ghariayal BO"
    ]
  },
  "384221": {
    "pincode": "384221",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Vadavali SO",
      "Chhamichha BO",
      "Maniyari BO",
      "Mithi Gariyal BO",
      "Takodi BO"
    ]
  },
  "384225": {
    "pincode": "384225",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Dhinoj SO",
      "Chaveli BO",
      "Danodarda BO",
      "Keshni BO",
      "Pimpal BO",
      "Sunsar BO",
      "Zilia BO"
    ]
  },
  "384229": {
    "pincode": "384229",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Lanva SO",
      "Mitha Dharva BO",
      "Palasar BO",
      "Pindharpura BO"
    ]
  },
  "384230": {
    "pincode": "384230",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Kamboi SO",
      "Delmal BO",
      "Itoda BO",
      "Sodhav BO",
      "Tambolia BO"
    ]
  },
  "384240": {
    "pincode": "384240",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Harij SO",
      "Aritha BO",
      "Bilia BO",
      "Boratwada BO",
      "Dhadhana BO",
      "Ekalva BO",
      "Jamanpur BO",
      "Jasomav BO",
      "Jasvantpura BO",
      "Juna Manka BO",
      "Kathi BO",
      "Katra BO",
      "Khakhal BO",
      "Kukrana BO",
      "Kumbhana BO",
      "Matrota BO",
      "Nana BO",
      "Nava Manka BO",
      "Piplana BO",
      "Ravad BO",
      "Ravindra BO",
      "Roda BO",
      "Sarval BO",
      "Tarora BO",
      "Vaghel BO",
      "Vagosan BO",
      "Vansa BO",
      "Vejawada BO"
    ]
  },
  "384241": {
    "pincode": "384241",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Mujpur SO",
      "Chandur Moti BO",
      "Jesda BO",
      "Khijdiyari BO",
      "Kunvar BO",
      "Kunvarad BO",
      "Lolada BO",
      "Loteshwar BO",
      "Memna BO",
      "Ranod BO",
      "Sipur BO",
      "Taranagar BO",
      "Tuvad BO"
    ]
  },
  "384245": {
    "pincode": "384245",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Sami SO",
      "Adgam BO",
      "Amrapuara BO",
      "Anvarpura BO",
      "Baspa BO",
      "Bhadrada BO",
      "Chandur Nani BO",
      "Dadar BO",
      "Daudpur BO",
      "Dudkha BO",
      "Gajdinpura BO",
      "Gujarwada BO",
      "Kanij BO",
      "Mandvi BO",
      "Mota Joravarpura BO",
      "Nayka BO",
      "Rafu BO",
      "Ranavada BO",
      "Samsherpura BO",
      "Varana BO",
      "Ved BO",
      "Zilvana BO"
    ]
  },
  "384246": {
    "pincode": "384246",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Sankheshwar SO",
      "Bolera BO",
      "Dantisana BO",
      "Dhanora BO",
      "Padla BO",
      "Panchasar BO"
    ]
  },
  "384255": {
    "pincode": "384255",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Kungher SO",
      "Adiya BO",
      "Chandrumana BO",
      "Khanpur Rajkuva BO",
      "Khari Vavdi BO",
      "Kureja BO",
      "Sankra BO"
    ]
  },
  "384260": {
    "pincode": "384260",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Manund SO",
      "Kanthravi BO",
      "Pali BO"
    ]
  },
  "384265": {
    "pincode": "384265",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Patan City SO",
      "Patan H N G University SO",
      "Patan Rajkawada SO",
      "Patan Siddhraj Road SO",
      "Patan Three Gate SO",
      "Patan HO"
    ]
  },
  "384266": {
    "pincode": "384266",
    "circle": "Gujarat circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Aghar BO",
      "Dharpur BO",
      "Kansa BO",
      "Odhva BO",
      "Pachakvada BO",
      "Dhanawada BO",
      "Hansapur BO",
      "Kalyana BO",
      "Katra Samal BO",
      "Kunwara BO",
      "Nayta BO",
      "Vamaiya BO",
      "Dharnoj BO",
      "Dunawada BO",
      "Rampura BO",
      "Kamliwada BO",
      "Sagodiya BO",
      "Vadli BO",
      "Dasawada BO",
      "Rajpur BO",
      "Sujnipur BO",
      "Ajimana BO",
      "Kotavad BO",
      "Mesar BO",
      "Anawada BO",
      "Runi BO"
    ]
  },
  "384272": {
    "pincode": "384272",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Sariyad SO",
      "Bepadar BO",
      "Goliwada BO",
      "Sampra BO",
      "Undra BO",
      "Veloda BO"
    ]
  },
  "384275": {
    "pincode": "384275",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Ranuj SO",
      "Borsan BO",
      "Ganget BO",
      "Islampura BO",
      "Matpur BO",
      "Norta BO",
      "Ruvavi BO",
      "Samoda BO",
      "Sander BO",
      "Sankhari BO",
      "Vasai BO"
    ]
  },
  "384285": {
    "pincode": "384285",
    "circle": "Gujarat circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Koita BO",
      "Laxmipura BO",
      "Morpa BO",
      "Muna BO",
      "Raviyana BO",
      "Vadu BO",
      "Vahana BO",
      "Vayad BO",
      "Wagdod SO",
      "Abluva BO",
      "Bhatsan BO",
      "Bhilvan BO",
      "Charup BO",
      "Dharusan BO",
      "Endla BO",
      "Jangral BO",
      "Khareda BO",
      "Khodana BO",
      "Kimbuva BO"
    ]
  },
  "384290": {
    "pincode": "384290",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Patan Division",
    "offices": [
      "Kakoshi SO",
      "Dindrol BO",
      "Methan BO",
      "Metrana BO",
      "Mudvada BO",
      "Sahesa BO"
    ]
  },
  "384305": {
    "pincode": "384305",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Kada SO Mahesana",
      "Dadhial BO",
      "Kharvada BO",
      "Lacjjadi BO",
      "Magroda BO"
    ]
  },
  "384310": {
    "pincode": "384310",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Valam SO",
      "Khandosan BO",
      "Pudgam BO",
      "Tarabh BO"
    ]
  },
  "384315": {
    "pincode": "384315",
    "circle": "Gujarat circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Ralisana BO",
      "Rangakui BO",
      "Saduthala BO",
      "Visnagar Market Yard SO",
      "Visnagar SO",
      "Visnagar Station Road SO",
      "Bhalak BO",
      "Denap BO",
      "Gothva BO",
      "Gunja BO",
      "Jaska BO",
      "Kamana BO",
      "Kansa BO",
      "Kuvasana BO",
      "Laxmipura BO",
      "Paladi BO",
      "Rajgadh BO"
    ]
  },
  "384320": {
    "pincode": "384320",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Umta SO"
    ]
  },
  "384325": {
    "pincode": "384325",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Kheralu SO",
      "Balad BO",
      "Chada BO",
      "Chansol BO",
      "Chotia BO",
      "Dabhad BO",
      "Dabhoda BO",
      "Dalisana BO",
      "Davol BO",
      "Keshimpa BO",
      "Mahiyal BO",
      "Mandropur BO",
      "Moti Hirvani BO",
      "Nanivada BO",
      "Rasulpur BO",
      "Sagthala BO",
      "Varetha BO",
      "Vithoda BO"
    ]
  },
  "384330": {
    "pincode": "384330",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Satlasana SO",
      "Ankaliyara BO",
      "Bedasma Mumanvas BO",
      "Bhalusana BO",
      "Bhatvas BO",
      "Bhimpura BO",
      "Gothada BO",
      "Kothasana BO",
      "Mothibhalu BO",
      "Sartanpur BO",
      "Tarangaji Temple BO",
      "Timba BO"
    ]
  },
  "384335": {
    "pincode": "384335",
    "circle": "Gujarat Circle",
    "region": "Ahmedabad HQ Region",
    "division": "Mahesana Division",
    "offices": [
      "Sipor SO",
      "Ambavada BO",
      "Gorisana BO",
      "Karsanpura BO",
      "Madhasana BO",
      "Sobhasan BO",
      "Unad BO",
      "Undhai BO",
      "Vaghawadi BO",
      "Valasana BO"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()

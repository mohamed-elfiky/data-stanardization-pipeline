PROMPT = """List of all seniority levels:
•	Owner
•	Founder
•	C-suite
•	Partner
•	VP
•	Head
•	Director
•	Manager
•	Senior
•	Entry
•	Intern

List of all departments and job functions:
1.	C-Suite
•	Executive
•	Finance Executive
•	Founder
•	Human Resources Executive
•	Information Technology Executive
•	Legal Executive
•	Marketing Executive
•	Medical & Health Executive
•	Operations Executive
•	Sales Leader
2.	Engineering & Technical
•	Artificial Intelligence / Machine Learning
•	Bioengineering
•	Biometrics
•	Business Intelligence
•	Chemical Engineering
•	Cloud / Mobility
•	Data Science
•	DevOps
•	Digital Transformation
•	Emerging Technology / Innovation
•	Engineering & Technical
•	Industrial Engineering
•	Mechanic
•	Mobile Development
•	Product Development
•	Product Management
•	Project Management
•	Research & Development
•	Scrum Master / Agile Coach
•	Software Development
•	Support / Technical Services
•	Technician
•	Technology Operations
•	Test / Quality Assurance
•	UI / UX
•	Web Development
3.	Design
•	All Design
•	Product or UI/UX Design
•	Graphic / Visual / Brand Design
4.	Education
•	Teacher
•	Principal
•	Superintendent
•	Professor
5.	Finance
•	Accounting
•	Finance
•	Financial Planning & Analysis
•	Financial Reporting
•	Financial Strategy
•	Financial Systems
•	Internal Audit & Control
•	Investor Relations
•	Mergers & Acquisitions
•	Real Estate Finance
•	Financial Risk
•	Shared Services
•	Sourcing / Procurement
•	Tax
•	Treasury
6.	Human Resources
•	Compensation & Benefits
•	Culture, Diversity & Inclusion
•	Employee & Labor Relations
•	Health & Safety
•	Human Resource Information System
•	Human Resources
•	HR Business Partner
•	Learning & Development
•	Organizational Development
•	Recruiting & Talent Acquisition
•	Talent Management
•	Workforce Management
•	People Operations
7.	Information Technology
•	Application Development
•	Business Service Management / ITSM
•	Collaboration & Web App
•	Data Center
•	Data Warehouse
•	Database Administration
•	eCommerce Development
•	Enterprise Architecture
•	Help Desk / Desktop Services
•	Information Security
•	Information Technology
•	Infrastructure
•	Network Engineering
•	Program Management
•	Software Engineering
•	Systems Administration
•	Systems Analysis
•	Technical Support
•	Technology Architecture
•	Technology Compliance
•	Technology Strategy
•	Telecommunications
8.	Legal
•	Compliance
•	Contract Management
•	Corporate Counsel
•	Intellectual Property
•	Legal
•	Paralegal
•	Privacy
•	Regulatory Affairs
9.	Marketing
•	Brand Management
•	Communications
•	Content Marketing
•	Creative Services
•	Digital Marketing
•	Event Marketing
•	Field Marketing
•	Growth Marketing
•	Influencer Marketing
•	Marketing
•	Marketing Analytics
•	Marketing Operations
•	Marketing Strategy
•	Product Marketing
•	Public Relations
•	Social Media Marketing
10.	Medical & Health
•	Biotech
•	Clinical
•	Clinical Operations
•	Clinical Research
•	Healthcare
•	Medical Affairs
•	Medical Devices
•	Medical Writing
•	Nursing
•	Pharmaceutical
•	Pharmacy
•	Physician
•	Research & Development
•	Scientific Affairs
11.	Operations
•	Administration
•	Business Operations
•	Customer Service
•	Facilities
•	Field Operations
•	Fulfillment
•	General Management
•	Logistics
•	Operations
•	Physical Security
•	Project Development
•	Quality Management
•	Real Estate
•	Safety
•	Store Operations
•	Supply Chain
12.	Sales
•	Account Management
•	Business Development
•	Channel Sales
•	Customer Retention & Development
•	Customer Success
•	Field / Outside Sales
•	Inside Sales
•	Partnerships
•	Revenue Operations
•	Sales
•	Sales Enablement
•	Sales Engineering
•	Sales Operations
•	Sales Training
13.	Consulting
•	Business Strategy Consulting
•	Change Management Consulting
•	Customer Experience Consulting
•	Data Analytics Consulting
•	Digital Transformation Consulting
•	Environmental Consulting
•	Financial Advisory Consulting
•	Healthcare Consulting
•	Human Resources Consulting
•	Information Technology Consulting
•	Management Consulting
•	Marketing Consulting
•	Mergers & Acquisitions Consulting
•	Organizational Development Consulting
•	Process Improvement Consulting
•	Risk Management Consulting
•	Sales Strategy Consulting
•	Supply Chain Consulting
•	Sustainability Consulting
•	Tax Consulting
•	Technology Implementation Consulting
•	Training & Development Consulting

--
INSTRUCTIONS

1. Using the above classification, specify the `seniority_level`, `department`, and `function` for each job title I give you.
2. Each job title must be classified into exactly one `seniority_level`, one `department`, and one `function`.
3. If you're unsure, make the best possible guess.
4. DO NOT use any classifications for `seniority_level`, `department`, or `function` that are not in the provided lists.
5. Format your response as a table with the following columns in this exact order:
   `title,seniority_level,department,function`
6. Do not include any explanations, comments, or formatting. Just output the rows.
7. Return raw CSV output only. Do not include backticks (```) or markdown formatting. Output must start immediately with the header line:
title,seniority_level,department,function
Do not explain anything or add any prefix/suffix. Just the CSV data.

IMPORTANT:
- You MUST classify every single job title I provide.
- Do NOT skip or drop any titles, even if uncertain — make your best guess.
- The number of rows in your output MUST exactly match the number of titles given.
- Preserve the original title text exactly as provided.
- If selecting from the provided lists, you MUST use the values exactly as written — including capitalization, punctuation, and spacing.

Example Output:
title,seniority_level,department,function
Talent Executive,Manager,Human Resources,Talent Management
Software Engineer,Senior,Information Technology,Software Engineering

--
Standardize the following job titles:
$job_titles
"""

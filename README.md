# ความแข็งแกร่งของแต่ละประเทศในกีฬาปิงปอง

รวมข้อมูลเหรียญรางวัลปิงปองชิงแชมป์โลก เหรียญโอลิมปิก และอันดับโลก WTT เป็นชุดข้อมูลเดียวในระดับประเทศ ด้วย Apache Airflow DAG จัดเก็บใน PostgreSQL และแสดงผลบนเว็บ dashboard (Streamlit) ที่อ่านจาก PostgreSQL โดยตรง

รายวิชา 02739452 แพลตฟอร์มข้อมูลขนาดใหญ่และการวิเคราะห์

## แหล่งข้อมูล

| # | แหล่ง | วิธีดึง | ข้อมูลที่ได้ |
|---|---|---|---|
| 1 | [Wikipedia: List of World Table Tennis Championships medalists](https://en.wikipedia.org/wiki/List_of_World_Table_Tennis_Championships_medalists) | Web scraping (`requests` + BeautifulSoup) | ตารางเหรียญรายประเทศ 5 รายการ: ชายเดี่ยว หญิงเดี่ยว ชายคู่ หญิงคู่ คู่ผสม |
| 2 | [Olympedia: Table Tennis](https://www.olympedia.org/sports/TTE) | Web scraping (`requests` + BeautifulSoup) | ตารางเหรียญโอลิมปิกตลอดกาล แยกตามรหัสประเทศ (NOC) |
| 3 | WTT Rankings API (`wttcmsapigateway-new.azure-api.net/ttu/Rankings/GetRankingIndividuals`) | REST API ไม่ต้องใช้ key | อันดับโลกชายเดี่ยวและหญิงเดี่ยว 1,000 อันดับแรก รวมเป็นรายประเทศ |

- ตัว scraper ตรวจ `robots.txt` ของทุกเว็บก่อนดึง (`ensure_allowed` ใน [dags/tt_pipeline/sources.py](dags/tt_pipeline/sources.py)) และส่ง User-Agent ที่ระบุตัวตน
- **PDPA:** ไม่เก็บข้อมูลส่วนบุคคล ชื่อและรหัสนักกีฬาจาก WTT ถูกตัดทิ้งตั้งแต่ขั้นดึงข้อมูล เหลือเพียงจำนวนนักกีฬา อันดับดีที่สุด และคะแนนรวมของแต่ละประเทศ จาก Olympedia ดึงเฉพาะตารางเหรียญรายประเทศ

## วิธีรัน

ต้องมี Docker Desktop (หรือ Docker Engine) พร้อม Docker Compose

```bash
cp .env.example .env
docker compose up --build -d
```

| บริการ | URL | หมายเหตุ |
|---|---|---|
| Airflow | http://localhost:8080 | ผู้ใช้และรหัสผ่านตาม `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` ใน `.env` |
| Dashboard | http://localhost:8501 | หน้า Dashboard และหน้า Pipeline status |
| PostgreSQL | `localhost:5433` | ฐานข้อมูลตาม `DATA_DB_*` ใน `.env` |

DAG `table_tennis_pipeline` ไม่ถูก pause และตั้ง `schedule="@daily"` จึงรันเองหนึ่งรอบหลัง Airflow เริ่มทำงาน (ประมาณ 1–2 นาที) กดรันซ้ำได้จากปุ่ม Trigger DAG ในหน้า Airflow

ตรวจว่ารันซ้ำแล้วจำนวนแถวไม่เพิ่ม:

```bash
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM country_stats"'
```

## โครงสร้าง DAG

```text
create_tables
  -> extract (TaskGroup: งานดึงข้อมูลทั้งหมดรันขนานกัน)
       scrape_wikipedia_world_championships
       scrape_olympedia_olympics
       fetch_wtt_ranking[MS, WS]          (Dynamic Task Mapping)
  -> merge_sources                         (trigger_rule=all_done: แหล่งใดล่มก็รวมแหล่งที่เหลือต่อ)
  -> check_quality                         (branch)
       -> quarantine_bad_rows -> load_postgres   เมื่อมีแถวไม่ผ่าน
       -> load_postgres                          เมื่อผ่านทั้งหมด
  -> validate_database                     (อย่างน้อย 100 แถว และไม่มีคีย์ซ้ำ)
  -> record_run                            (บันทึกสถานะสำหรับหน้า Pipeline status)
```

## ตารางใน PostgreSQL

| ตาราง | คีย์หลัก | ใช้ทำอะไร |
|---|---|---|
| `country_stats` | `(source, event, country_code)` | schema เดียวสำหรับทุกแหล่ง โหลดด้วย UPSERT (`ON CONFLICT DO UPDATE`) |
| `countries` | `country_code` | หนึ่งแถวต่อประเทศหลังตัดรายการซ้ำข้ามแหล่ง พร้อมรายชื่อแหล่งที่พบ |
| `quality_issues` | `(source, event, country_name)` | แถวที่ไม่ผ่านการตรวจคุณภาพ |
| `pipeline_runs`, `source_runs` | `run_id` | สถานะของแต่ละรอบและแต่ละแหล่ง |
| `country_summary` (view) | | สรุปรายประเทศจากทุกแหล่ง สำหรับ dashboard |

## การรวมและตรวจคุณภาพข้อมูล

- **ตัดรายการซ้ำข้ามแหล่ง:** Wikipedia ใช้ชื่อประเทศ ส่วน Olympedia และ WTT ใช้รหัส 3 ตัว จึงแปลงชื่อจาก Wikipedia เป็นรหัสด้วย `WIKIPEDIA_COUNTRY_CODES` ใน [dags/tt_pipeline/transform.py](dags/tt_pipeline/transform.py) แล้วรวมเป็นหนึ่งแถวต่อรหัสในตาราง `countries` เช่น "China" (Wikipedia), "People's Republic of China" (Olympedia) และ "China" (WTT) รวมเป็น `CHN`
- **ทีมในอดีต** เช่น สหภาพโซเวียต (`URS`), เยอรมนีตะวันตก (`FRG`), เชโกสโลวาเกีย (`TCH`) ใช้รหัสของตัวเอง ไม่รวมเข้ากับประเทศปัจจุบัน
- **เหรียญครึ่งเหรียญ:** ในประเภทคู่ คู่ที่มาจากสองประเทศแบ่งเหรียญกัน Wikipedia จึงแสดงค่าเช่น 11.5 คอลัมน์เหรียญจึงเป็น `NUMERIC(6,1)`
- **กฎตรวจคุณภาพ:** แปลงชื่อประเทศเป็นรหัสได้, จำนวนเหรียญไม่ติดลบ, ทอง + เงิน + ทองแดง = รวม, อันดับโลก ≥ 1 แถวที่ไม่ผ่านไปอยู่ใน `quality_issues` ส่วนแถวที่ผ่านโหลดตามปกติ

## โครงสร้างไฟล์

```text
dags/table_tennis_pipeline.py   Airflow DAG
dags/tt_pipeline/sources.py     ตัวดึงข้อมูลทั้ง 3 แหล่ง
dags/tt_pipeline/transform.py   รวม schema แปลงรหัสประเทศ และตรวจคุณภาพ
dags/tt_pipeline/db.py          สร้างตารางและ UPSERT
dags/sql/schema.sql             ตาราง และ view สำหรับ dashboard
web/app.py                      Streamlit dashboard
airflow/                        Dockerfile และสคริปต์เริ่ม Airflow
docker-compose.yml
requirements.txt                แพ็กเกจของ DAG (web/requirements.txt สำหรับ dashboard)
.env.example
```

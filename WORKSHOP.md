# الورشة — 50 دقيقة

**الفكرة:** نفس القواعد تشتغل بطريقتين: لحظيًا (Kafka) ويوميًا (Dagster)، وتعطي نفس النتيجة. والدليل اختبار.
الشاشة طوال الوقت: صفحة الـpipeline البصري. كل الأرقام فيها حقيقية.

| الوقت | ماذا يحدث | على الشاشة |
|---|---|---|
| 0–5 | **المشكلة.** شركة نقل: ورقة الطريق تقول رحلة 08:18، فهل حدثت؟ ومتى؟ وفي أي اتجاه؟ ورقة مكتوبة باليد مقابل GPS كل 10 ثوان. | جدول "حافلة A": المكتوب مقابل GPS |
| 5–12 | **المفاهيم في المخطط نفسه.** المسار العلوي = streaming، السفلي = batch. Kafka سجل (topic، key = الحافلة، offset، consumer group). Dagster أصول يومية بفحوص. | الصفحة فارغة النشاط |
| 12–22 | **عرض حي 1: البث.** بداية نظيفة ← live_trips ▶ ← إعادة اليوم ×120. النقاط تتدفق، الرحلات تظهر في القائمة لحظة وصول الحافلة للمحطة. | العقد العلوية خضراء، القائمة تمتلئ |
| 22–27 | **عرض حي 2: التأخر وإعادة القراءة.** ⏸ live_trips: الرسائل تتراكم في Kafka، "تأخر" يكبر على العقدة. ▶ من جديد: يلحق دون أن يضيع شيء. هذه قيمة Kafka. | رقم التأخر يصعد ثم ينزل |
| 27–35 | **عرض حي 3: الـbatch.** تشغيل الـbatch ليوم 19: الأصول تضيء واحدًا واحدًا، ثم الفحوص الأربعة. النتيجة: 668 رحلة، 71% مؤكدة، 75% كم تجاري. | المسار السفلي، لوحة النتيجة |
| 35–39 | **عرض حي 4: Superset.** لوحة الأسبوع (13–18) مفتوحة؛ بعد انتهاء الـbatch يظهر يوم 19 فيها. من البيانات الخام إلى لوحة الإدارة في سلسلة واحدة. | Superset |
| 39–43 | **الكود (مختصر).** اضغط gps_trips (Polars)، ثم live_trips (`fleet/live.py`): نفس القواعد. ثم الاختبار الذي يعيد اليوم نقطة نقطة ويطابق الـbatch. اضغط device_identity: الـER، الأسماء تكذب والسلوك يقرر. | نافذة الكود |
| 43–47 | **ما تعلمناه من البيانات.** أوقات بقالب "+15 دقيقة"، ذهاب/إياب صحيح في 52–65% فقط، جهاز باسم حافلة في مدينة أخرى. القرار لإنسان. | — |
| 47–50 | أسئلة | — |

## قبل الورشة (الليلة)

1. `docker compose up -d` على الجهاز الذي ستعرض منه، وجرّب الأزرار كلها مرة. مسار Kafka جُرّب (`tests/test_kafka_e2e.py`). السرعة ×120 أو ×300 يلحق بها live_trips؛ ×600 تُظهر التأخر وحدها.
2. `docker compose pull` مسبقًا: لا تعتمد على إنترنت القاعة.
3. شغّل الـbatch مرة قبل البدء (يخلق `lines` و`fleet_registry`)، ثم "بداية نظيفة".
   Live uses the latest resolved identity from the last batch run (`data/identity/latest.json`, kept by the reset).
4. `.env`: `VIZ_ANONYMIZE=1`، `FLEET_POSITIONS_PREFER=inbox`، `VIZ_ADMIN_TOKEN`.
5. لا Traccar حي ولا API حي على المسرح: الإعادة فقط. لا أرقام حافلات، لا أرقام مستلمين، لا شيء عن التحقيق.

## Slides

14 slides in English, with timing cues in the speaker notes: https://claude.ai/artifact/SfWoAAuYbvqfWGQ3Y9cQjc
(private until shared from its Share menu; exports to PDF or PowerPoint there). Source: `docs/slides/`.

## On the VPS before the talk

`./run-vps.sh prepare`: starts the stack, starts the Dagster web UI (https://dagster.devcloud.dz, user `fleet`,
password in `/home/ploi/fleet-secrets/dagster-password.txt`), resets the demo, prints memory. Open the page once as
`https://pipeline.devcloud.dz/#admin=<VIZ_ADMIN_TOKEN>` on the presenting laptop. Details: `deploy/DEPLOY.md`.

## Timing at ×120 from 05:30 (measured on the VPS)

The whole replay takes 9 min 17 s (288,900 positions). Off-route alerts appear on the page (real time after pressing replay):

| Bus on the page | Off route since | Appears after |
|---|---|---|
| Bus R | 06:24 | 0:35 |
| Bus O | 06:41 | 0:43 |
| Bus X | 06:54 | 0:50 |
| Bus G | 07:04 (opens 07:29) | 1:03 |
| Bus O | 07:53 | 1:20 |
| Bus R (22606, school transport) | 14:12 | 4:30 |

## بعد البروفة: الرجوع إلى نقطة البداية

البيانات لا تُنشأ من جديد. يوم 19 نوفمبر يُعاد كما هو في كل مرة، فتعرف مسبقًا ما سيظهر.
ما يُمسح هو فقط ما يصنعه العرض:

```bash
python -m tools.reset_demo --kafka                                        # على الحاسوب
./run-vps.sh reset                                                        # على الـVPS
```

| يبقى | يُمسح |
|---|---|
| `data/inbox`: المواقع والأوراق الحقيقية | الحالة الحية: الرحلات والتنبيهات في القائمة |
| نتائج أيام 13–18 في PostgreSQL، أي أسبوع Superset | نتائج يوم 19 (ملفات الـwarehouse وصفوف PostgreSQL)، ليُشغَّل الـbatch أمام الحضور |
| صور Docker ولوحة Superset وتاريخ Dagster | الموضع الجزئي لليوم في الـlake، ورسائل Kafka (مع `--kafka`) |

بعده تأكد: المسار العلوي ساكن، والسفلي رمادي ليوم 19، وSuperset يعرض 13–18 فقط.

## إن تعطل شيء على المسرح

| العطل | الحل |
|---|---|
| Kafka لا يعمل | زر "بدون Kafka": نفس المنطق من الملف بنفس السرعة. قلها صراحة: هذا هو الوضع البديل. |
| الصفحة لا تفتح | `python -m stream.live_trips --file data/inbox/positions_2026-11-19.csv.gz --speed 300 --from 05:30` في الطرفية |
| الـbatch بطيء | النتائج محسوبة مسبقًا في `data/warehouse`: العقد السفلية خضراء أصلًا |

import sys
# المكتبة دي بتديك تعامل مباشر مع النظام (Operating System)

import pandas as pd
# دي مكتبة مشهورة جدًا للتعامل مع الجداول والبيانات (زي Excel كده بس بالكود).

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
# Dataframe هتخلق جدول مكون من عمودين

print(df.head())
# بتعرض أول 5 صفوف من الجدول

df.to_parquet(f"output_{sys.argv[1]}.parquet")
# هتخزن الجدول اللي اسمه df في ملف Parquet (نوع ملف سريع ومضغوط للبيانات).
# اسم الملف هيتاخد من ال argument[1] اللي انت هتدخله، argv[0] = pipeline.py and argv[1] = الللي هتكتبه بعده

month = int(sys.argv[1])

print("Arguments", sys.argv)
# Argument دي ليست (List) فيها كل الحاجات اللي اتبعتت للبرنامج وقت التشغيل

print(f"Running pipeline for month {month}")
# حرف ال f اللي قبل علامة ال " اختصار ل formatted string بيخليك تضيف المتغير جوا ال "" عادي عن طريق إنك تحطه في ال {} دي
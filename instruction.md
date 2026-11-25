UWAGA: - Projekt musi być opracowany z wykorzystaniem silnika i platformy Apache Spark do przetwarzania danych na dużą skalę (w tym z wykorzystaniem przynajmniej jednej z bibliotek: Spark Streaming i/lub uczenia maszynowego Mllib).
W projekcie należy wykorzystać brokerów Apache Kafka przeznaczonych do rozproszonego przetwarzania strumieni danych która ma współpracować z opracowaną aplikacją back-end i front-end (wykonanie aplikacji backend i frontend może być w dowolnej technologii) 
oraz z zastosowaniem następujących narzędzi:
- PyCharm - zintegrowanego środowiska programistycznego przeznaczonego do języka Python
W analizie danych - proszę wykorzystać zestaw narzędzi Jupyter do interaktywnych obliczeń opartych na notatnikach (.ipynb), łączących kod, tekst, dane oraz wizualizacje. 
Dopuszczalne jest wykonanie wstępnej EDA z wykorzystaniem:
- Google Colab --> platformy hostowana usługa (np. Jupyter Notebook), która nie wymaga żadnej konfiguracji i zapewnia bezpłatny dostęp do zasobów obliczeniowych, w tym procesorów GPU i TPU., Istotne jest również graficzne prezentowanie wyników.
- Azure Databricks --> platformy analityki i sztucznej inteligencji oparta na chmurze, zoptymalizowana pod kątem platformy Microsoft Azure, która łączy w sobie możliwości Apache Spark i Delta Lake, zapewniając ujednolicone środowisko współpracy do przetwarzania dużych zbiorów danych, inżynierii danych i uczenia maszynowego,
Kod projektu, strojenia oraz porównania różnych modeli - proszę przeprowadzić w środowisku PyCharm lub środowisku właściwym do budowy aplikacji w zależności od przyjętej technologii po stronie backend i frontend.

Wymagania:
-----------
1) Opis "Celu projektu" - założeń projektowych poczynionych przy wyborze tematu,
a. przyjąć i postawić tezy dla procesu przetwarzania danych w celu uzyskania na ich podstawie informacji i wniosków,
b. w zależności od stawianych problemów, założyć określone użycie metod statystycznych, eksploracyjnych i innych oraz sposobów wykorzystania do tego podanych narzędzi.
2) Dobór danych do postawionego celu.
a. na potrzeby wykonywanego projektu wykonać opis doboru danych do przyjętego tematu, ich natywnej struktury, formatów,
b. opisać charakter ilościowy i jakościowy danych surowych, ich sposób powstania (dane pierwotne, dane wtórne), dynamikę zmian danych (ciągła rejestracja zdarzeń, rejestracja w interwałach czasowych, rejestracja jednorazowa), źródeł pochodzenia.
c. opisać proces pozyskiwania danych w postaci strumieni danych (dla odbywających się zdarzeń w czasie rzeczywistym), które są udostępniane za pośrednictwem brokerów Apache Kafka
3) Ekstrakcja i Analiza danych
a. omówić:
- procesy porządkowania, przekształcania i modelowania danych w celu zdobycia użytecznych informacji, wypracowania wniosków,
- wykorzystywane techniki i narzędzia do znajdowania ukrytych dla człowieka (z uwagi na ograniczone możliwości czasowe) prawidłowości w zgromadzonych danych,
b. opracować wymagane skrypty konieczne do wykonania zadań ekstrakcji, transformacji i ładowania przetworzonych danych do formatów o zdefiniowanej strukturze z użyciem Apache Spark.
c. omówić proces uczenia maszynowego na klastrze Apache Spark:
- proces budowy przykładowego modelu: np.  systemu klasyfikacyjnego, predykcyjnego lub rekomendacyjnego
4) Podać uwagi i wnioski końcowe w następstwie przeprowadzonej analizy i w jaki sposób mogą być one użyteczne w procesie wspierania procesu decyzyjnego?
UWAGA:

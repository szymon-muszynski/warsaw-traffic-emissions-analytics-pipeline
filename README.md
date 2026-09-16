# Warsaw Air Quality vs. Fleet Modernization — PySpark & Delta Lake on Databricks 🚗🌫️

Analiza zależności między jakością powietrza w centrum Warszawy a postępującą modernizacją floty samochodowej (wzrost udziału pojazdów niskoemisyjnych), zrealizowana w środowisku **Databricks** przy użyciu **PySpark** i **Delta Lake**.

> ⚠️ **Uwaga dotycząca uruchomienia:** Ten projekt został w całości zbudowany i uruchomiony w środowisku **Databricks Community Edition** — korzysta z natywnego systemu plików Databricks (DBFS), silnika Spark dostarczanego przez klaster Databricks oraz formatu tabel **Delta Lake**. Notatniki **nie uruchomią się** poprzez proste sklonowanie repozytorium i odpalenie lokalnie (np. w Jupyterze na własnym komputerze) — wymagają środowiska Databricks (lub kompatybilnego z nim runtime Spark + Delta Lake) oraz wgrania plików źródłowych do przestrzeni roboczej. Repozytorium ma charakter dokumentacyjno-portfolio: prezentuje kod, wyniki i proces analityczny, nie jest to instrukcja uruchomienia.

## 📌 Pytanie badawcze

Czy postępująca modernizacja floty samochodowej w Warszawie (rosnący udział pojazdów niskoemisyjnych) znajduje odzwierciedlenie w poprawie jakości powietrza w centrum miasta, mierzonej poziomem dwutlenku azotu (NO2)?

## 🧪 Dlaczego NO2, a nie PM10/PM2.5?

Pyły zawieszone (PM10, PM2.5) w polskich miastach w dużej mierze pochodzą z ogrzewania budynków (tzw. niska emisja) i działalności przemysłowej, nie tylko z transportu — użycie ich do tej analizy zaburzałoby wnioski o wpływie samego ruchu samochodowego. **NO2 jest znacznie silniej i bardziej bezpośrednio powiązany ze spalinami samochodowymi**, co czyni go czystszym wskaźnikiem do tego konkretnego porównania.

## 🗂️ Dane źródłowe

1. **Dane o jakości powietrza (GIOŚ - https://powietrze.gios.gov.pl/pjp/archives)** — godzinowe pomiary stężenia NO2 ze wszystkich stacji pomiarowych w Polsce, w podziale na 10 osobnych plików rocznych (2015–2024). Do analizy wykorzystano wyłącznie stację **Warszawa, Aleje Niepodległości (`MzWarAlNiepo`)** — stację zlokalizowaną w ścisłym centrum miasta, celowo wybraną zamiast stacji na obrzeżach, aby uchwycić wpływ ruchu miejskiego.
2. **Dane o flocie samochodowej w Warszawie (GUS - https://bdl.stat.gov.pl/bdl/metadane/cechy/3583)** — roczna liczba zarejestrowanych pojazdów osobowych w Warszawie (2015–2024), w rozbiciu na rodzaj napędu: benzyna, olej napędowy, LPG oraz kategoria "pozostałe".

> ⚠️ **Ograniczenie metodologiczne:** Kategoria "pozostałe" w danych GUS obejmuje pojazdy o napędzie innym niż benzynowy, diesel i LPG. W analizowanym okresie (2015–2024) to w przeważającej mierze pojazdy elektryczne i hybrydowe, jednak dostępne dane nie pozwalają na precyzyjne rozdzielenie tych kategorii — traktowana jest ona jako przybliżenie udziału floty niskoemisyjnej, nie jako dokładna liczba samych EV/hybryd.

Zakres czasowy analizy (2015–2024) został podyktowany dostępnością danych o flocie samochodowej.

## 🛠️ Wykorzystane technologie

- **Databricks** (Community Edition) — środowisko notebooków z zarządzanym klastrem Apache Spark.
- **PySpark** — wczytywanie, czyszczenie i transformacja danych; cały proces ETL zrealizowany na DataFrame API i Spark SQL.
- **Delta Lake** — format przechowywania tabel pośrednich i wynikowych (transakcyjność ACID, wydajne odczyty).
- **Matplotlib** — wizualizacja finalnych wyników.

## 🏗️ Architektura (Medallion: Bronze → Silver → Gold)

### Bronze (Raw)
10 surowych plików CSV z danymi o NO2 (jeden na rok, 2015–2024) oraz jeden plik CSV z danymi o flocie samochodowej w Warszawie, wgrane bez modyfikacji do przestrzeni roboczej Databricks.

### Silver (Cleaned)

**Dane o powietrzu (`silver_no2_warsaw`):**
- Pliki źródłowe miały niespójną strukturę między latami — plik z 2015 roku miał uboższy nagłówek niż kolejne lata (brakowało m.in. numeracji porządkowej stacji), co wymagało dostosowania logiki wczytywania.
- Każdy plik wczytywany był bez nagłówka (kolumny jako `_c0`, `_c1`, ...), a kolumna odpowiadająca stacji `MzWarAlNiepo` była **dynamicznie wykrywana** poprzez przeszukanie próbki wierszy — niezależnie od tego, pod jakim numerem kolumny stacja znajdowała się w danym roczniku.
- Wiersze zawierające urzędowe metadane (nagłówki typu "Wskaźnik", "Jednostka", "Kod stacji") zostały odfiltrowane wyrażeniem regularnym, pozostawiając wyłącznie wiersze zaczynające się od poprawnego formatu daty (`DD.MM.RRRR`).
- Wartości NO2 zapisane z przecinkiem dziesiętnym zostały przekonwertowane na format zmiennoprzecinkowy (`regexp_replace` + `cast("float")`).
- Wiersze z brakującymi lub błędnymi odczytami (np. tekstowe wartości niemożliwe do skonwertowania na liczbę) zostały odrzucone (`dropna`) — przy blisko 80 tysiącach rekordów godzinowych utrata pojedynczych, uszkodzonych pomiarów nie wpływa istotnie na wyniki, a sztuczne uzupełnianie (imputation) zniekształciłoby rzeczywiste piki w godzinach szczytu.
- **Wartości odstające (outliery) celowo nie zostały odfiltrowane** — w kontekście jakości powietrza skrajnie wysokie odczyty rzadko są błędem pomiarowym, a częściej rzeczywistym, chwilowym obrazem silnego zanieczyszczenia. Przy uśrednianiu rocznym z tysięcy pomiarów pojedyncze ekstrema nie zaburzają długoterminowego trendu.

**Dane o flocie (`silver_cars_warsaw`):**
- Plik źródłowy miał strukturę "szeroką" (jeden wiersz z osobnymi kolumnami dla każdej kombinacji rok × typ napędu) — został przekształcony do formatu "długiego" (rok jako osobny wiersz) za pomocą wygenerowanego dynamicznie zapytania SQL (`UNION ALL` w pętli po latach).
- Obliczony został procentowy udział pojazdów z kategorii "pozostałe" w całkowitej flocie osobowej dla każdego roku.

### Gold (Aggregated)
Trzy tabele wynikowe, każda odpowiadająca jednemu etapowi analizy:
- `gold_hourly_profile` — średnie stężenie NO2 w podziale na godzinę doby (uśrednione z całego okresu 2015–2024).
- `gold_monthly_profile` — średnie stężenie NO2 w podziale na miesiące (rok 2023 jako reprezentatywny przykład).
- `gold_yearly_trend` — połączenie (join) rocznych średnich NO2 z procentowym udziałem floty niskoemisyjnej — główna tabela analityczna projektu.

## 📊 Wyniki i wnioski

### 1. Profil godzinowy NO2 (2015–2024)
<p align="center">
  <img width="840" height="470" alt="image" src="https://github.com/user-attachments/assets/aed0cd1e-a9f5-49dc-bdfd-e50e70b366fe" />
</p>
Wyraźnie widoczne dwa szczyty stężenia NO2 — poranny (ok. 7:00–9:00) i popołudniowy (ok. 15:00–20:00) — pokrywające się z godzinami dojazdów do i z pracy. Potwierdza to, że NO2 w centrum miasta jest silnie skorelowany z ruchem samochodowym, uzasadniając wybór tego wskaźnika do dalszej analizy.

### 2. Profil miesięczny NO2 (rok 2023)
<p align="center">
<img width="836" height="468" alt="image" src="https://github.com/user-attachments/assets/19355898-9caa-45d6-bcf1-f69d78729358" />
</p>

Stężenie NO2 wykazuje pewną sezonowość (wyższe wartości w miesiącach wiosennych i jesiennych), jednak wahania w obrębie roku są względnie umiarkowane. To wspiera decyzję o wykorzystaniu **średniej rocznej** jako miarodajnego przybliżenia poziomu zanieczyszczenia w danym roku w dalszej części analizy.

### 3. Zestawienie: NO2 a modernizacja floty (2015–2024)
<p align="center">
<img width="1182" height="587" alt="image" src="https://github.com/user-attachments/assets/10808831-c913-4091-9ca8-2e517e6ba4bb" />
</p>

Zestawienie rocznego trendu stężenia NO2 (malejącego) z rosnącym udziałem procentowym floty niskoemisyjnej. W analizowanym okresie widoczny jest wyraźny spadek NO2 przy jednoczesnym, systematycznym wzroście udziału pojazdów z kategorii "pozostałe" (proxy dla EV/hybryd).

**Ważne zastrzeżenie:** Dane przybliżają jedynie częściowo zjawisko stref czystego transportu i modernizacji floty — obserwowana korelacja nie stanowi dowodu na bezpośredni związek przyczynowo-skutkowy. Na spadek NO2 mogły wpływać także inne czynniki (np. zmiany w organizacji ruchu, pandemia COVID-19 widoczna jako wyraźny spadek w 2020 roku, ogólna poprawa norm emisji spalin). Wzrost udziału floty niskoemisyjnej jest jednak spójny z obserwowanym trendem i może stanowić jeden z czynników sprzyjających poprawie jakości powietrza.

**Uwaga dotycząca danych o flocie:** W 2024 roku odnotowano spadek całkowitej liczby zarejestrowanych pojazdów w Warszawie o ok. 200 tys. względem roku poprzedniego — wynika to z działań CEPiK polegających na usuwaniu z rejestru tzw. "martwych dusz" (pojazdów zarejestrowanych, lecz faktycznie nieużywanych), nie z rzeczywistego spadku liczby użytkowanych aut.

## 📁 Struktura repozytorium

```
.
├── README.md
├── cleaning.ipynb          # Bronze → Silver: czyszczenie i przygotowanie danych
├── analytics.ipynb         # Silver → Gold: agregacje, analiza i wizualizacje
├── raw_data/
      ├── 2015_NO2_1g.csv ... 2024_NO2_1g.csv
      └── cars_warsaw_2015-2024.csv
```
## 🎯 Cel projektu

Projekt powstał w celu praktycznego przećwiczenia pracy z **PySpark i Delta Lake w środowisku Databricks** — od wczytania i wyczyszczenia niespójnych, "brudnych" danych publicznych (różne formaty między rocznikami, błędy kodowania, separator dziesiętny, urzędowe metadane w plikach), przez zbudowanie warstwowej architektury danych (Bronze-Silver-Gold), po połączenie dwóch niezależnych źródeł danych w spójną analizę odpowiadającą na konkretne pytanie badawcze.

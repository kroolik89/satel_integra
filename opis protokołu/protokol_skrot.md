# Rozszerzona dokumentacja protokołu Satel Integra (ETHM-1 / INT-RS)
Na podstawie dokumentu: ETHM-1 Plus protocol description (2025-09-26)

## 1. Struktura Ramki (Function 2)
Używana do integracji.
`0xFE 0xFE [cmd] [d1] [d2] ... [dn] [crc_high] [crc_low] 0xFE 0x0D`

### Specjalne Bajty (Byte Stuffing):
* `0xFE 0xFE`: Synchronizacja (początek ramki).
* `0xFE 0xF0`: Mapowanie bajtu `0xFE` występującego wewnątrz danych (cmd, data, crc). Wtedy do CRC liczymy tylko raz `0xFE`.
* `0xFE 0x0D`: Koniec ramki.

## 2. Obliczanie CRC-16
1. Inicjalizacja: `crc = 0x147A`
2. Dla każdego bajtu `b` (cmd, d1...dn):
   * `crc = rotate_left(crc, 1)`
   * `crc = crc XOR 0xFFFF`
   * `crc = crc + crc_high + b`

## 3. Kodowanie i Szyfrowanie (ETHM-1 / INT-GSM)
*Dokument PDF wspomina o kluczach ETHM/GPRS (Zdarzenie 75), ale szczegółowy opis szyfrowania AES-128 (używanego w komunikacji z DLOADX/GUARDX przez sieć) jest często elementem rozszerzonym protokołu.*

**Uwaga techniczna dotycząca szyfrowania:**
W komunikacji sieciowej (ETHM-1 Plus) często stosuje się szyfrowanie kluczem zapisanym w centrali. Ramka danych przed wysłaniem jest dopełniana do wielokrotności 16 bajtów (padding) i szyfrowana AES. Jeśli dodajemy szyfrowanie, musimy uwzględnić:
- Klucz komunikacyjny (zdefiniowany w centrali).
- Szyfrowanie całego pakietu (pomiędzy nagłówkiem a końcówką).

## 4. Kluczowe Komendy (Odczyt stanu)
* `0x00`: Naruszenia wejść (16/32 bajty)
* `0x01`: Sabotaże wejść
* `0x02`: Alarmy wejść
* `0x07`: Problemy wejść ('no violation')
* `0x09`/`0x0A`: Uzbrojone strefy (4 bajty)
* `0x13`: Alarmy stref
* `0x17`: Stan wyjść (16/32 bajty)
* `0x1A`: RTC i bity statusu (9 bajtów: YYYY MM DD HH MM SS + status)
   * Bit statusu: .7=Service Mode, .6=Troubles, .5=Troubles Memory
* `0x1B..0x1F`: Problemy (Troubles) - szczegółowe listy awarii.
* `0x7C`: Wersja modułu ETHM-1/INT-RS (zwraca też flagi możliwości modułu).
* `0x7D`: Odczyt temperatury z czujnika (Zone 1..256). Zwraca 2 bajty: `temp = (high << 8 | low)`. Wartość 0.5 stopnia na bit, przesunięcie -55.0°C.
* `0x7E`: Wersja centrali INTEGRA (typ centrali, wersja firmware, język).
* `0x7F`: Lista nowych danych (odpytywanie o zmiany w komendach 0x00..0x2F).

## 5. Sterowanie (Wymaga kodu użytkownika)
Format kodu: `[kod_hex] [0xFF]...` (dopełnione do 8 bajtów). Np. kod 1234 to `0x12 0x34 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF`.

* `0x80`: Uzbrój (Tryb 0 - pełne) + 8B kod + 4B lista stref
* `0x81-0x83`: Uzbrój w trybach STAY (różne warianty).
* `0x84`: Wyłącz czuwanie + 8B kod + 4B lista stref
* `0x85`: Skasuj alarm + 8B kod + 4B lista stref
* `0x86`: Blokowanie wejść (Bypass) + 8B kod + 16/32B lista wejść.
* `0x88`: Włącz wyjścia + 8B kod + 16/32B lista wyjść.
* `0x89`: Wyłącz wyjścia.
* `0x8C`: Odczyt zdarzeń (Log) - wymaga indeksu zdarzenia. `0xFF 0xFF 0xFF` to ostatnie zdarzenie.
   * Zwraca 15 bajtów (nagłówek, rekord 8B, indeksy).
* `0x8E`: Ustawienie czasu (RTC) + 8B kod + 14B ASCII (yyyymmddhhmmss).

## 6. Wyniki Operacji (0xEF)
Zwracane po komendach sterujących:
* `0x00`: OK
* `0x01`: Błędny kod użytkownika
* `0x02`: Brak dostępu
* `0x11`: Nie można uzbroić (można wymusić komendą 0xA0)
* `0x12`: Nie można uzbroić
* `0xFF`: Komenda przyjęta do przetwarzania (proces trwa)

## 7. Typy Central (Komenda 0x7E)
* 0-3: Integra 24, 32, 64, 128
* 4: Integra 128-WRL (SIM300)
* 132: Integra 128-WRL (LEON)
* 66, 67, 72: Integra 64 Plus, 128 Plus, 256 Plus
* 8: Integra 256 Plus (kod statusu RTC)

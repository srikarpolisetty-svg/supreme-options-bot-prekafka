import yfinance as yf
import pandas as pd
import time

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

symbols = [
"AAPL","MSFT","AMZN","NVDA","GOOGL","GOOG","META","BRK-B","TSLA","UNH",
"XOM","JNJ","V","PG","JPM","AVGO","MA","HD","CVX","LLY",
"MRK","ABBV","PEP","COST","WMT","MCD","KO","BAC","PFE","TMO",
"ACN","CSCO","ABT","DHR","WFC","DIS","VZ","ADBE","CMCSA","NFLX",
"CRM","INTC","AMD","TXN","NEE","NKE","LIN","PM","RTX","HON",
"UPS","LOW","QCOM","AMGN","INTU","IBM","MS","GS","CAT","SBUX",
"SPGI","BLK","MDT","BA","GE","ISRG","DE","PLD","T","GILD",
"BKNG","LMT","CVS","AXP","TJX","ADP","SYK","AMAT","NOW","MMC",
"ZTS","MO","VRTX","C","EL","ADI","REGN","SCHW","MU","BDX",
"CI","DUK","SO","ITW","CB","PNC","EOG","CL","CSX","NSC",
"FIS","APD","ICE","HCA","FDX","EMR","GM","SHW","PSA","USB",
"WM","MCO","ETN","AON","TGT","KMB","ORCL","SLB","BSX","NOC",
"APTV","EW","ROP","MAR","AIG","KMI","WELL","F","ADM","MPC",
"TRV","SRE","D","STZ","MSI","OXY","HLT","PGR","LRCX","EXC",
"PEG","KR","VLO","ALL","AEP","AFL","CTAS","PRU","ROST","WBA",
"ED","PPG","PCAR","YUM","IDXX","KHC","DLR","CDNS","TT","PH",
"ANET","SNPS","FTNT","CTSH","MTD","BAX","PAYX","DOW","ODFL","ILMN",
"BIIB","XEL","OTIS","AZO","CMG","WMB","VRSK","VMC","IQV","ALB",
"RMD","DHI","CARR","SBAC","AVB","ARE","ECL","FAST","GPN","HIG",
"VTR","GLW","DAL","EBAY","LEN","LYB","ZBRA","TSCO","MLM","WDC",
"EXR","EFX","XYL","A","CHTR","FANG","KEYS","ULTA","CINF","FE",
"STT","DFS","PXD","CFG","CAH","NUE","HAL","NEM","RF","FITB",
"VTRS","WRB","MKC","VFC","CTVA","FISV","WST","ESS","MPWR","PAYC",
"STE","BBY","GWW","TYL","HSY","RSG","HPE","ZBH","INVH","NVR",
"TPR","SYY","IEX","LHX","SWKS","VRSN","AES","AEE","PKG","NDAQ",
"CMS","EXPD","BR","FMC","HOLX","ATO","OMC","PEAK","J","K","RE",
"AMCR","TXT","NRG","BF-B","UAL","L","BKR","CE","COO","DRI",
"EXPE","LUV","HWM","IP","MAS","CNP","DG","FRT","MGM","PHM",
"POOL","VICI","SNA","RCL","HAS","WHR","FOX","FOXA","TAP","UHS",
"MTCH","DVA","AOS","CPRT","CDW","ROL","GRMN","TRMB","WAT","NDSN",
"CHRW","ETSY","KMX","ALGN","LW","BIO","IRM","CBOE","KEY","KIM",
"PNR","RJF","WRK","NWL","MHK","AAL","SEE","KSS","NI","APA",
"IPG","AKAM","HII","FTV","NTRS","UAA","UA","QRVO","DXCM","MOS",
"TER","CAG","MKTX","BWA","BEN","PFG","ZION","IVZ","HBAN","CMA",
"JKHY","LNT","VNO","BBWI","ALLE","GNRC","TECH","TROW","PKI",
"ENPH","SEDG","FSLR","ON","SMCI","PLTR","RIVN","SNOW","NET","DDOG",
"ZS","OKTA","TEAM","DOCU","SHOP","SQ","PYPL","COIN","ROKU","SPOT",
"UBER","LYFT","ABNB","MDB","WDAY","PANW","CRWD","TWLO","SPLK","ZM",
"WMT","COST","TGT","DG","DLTR","KR","SFM","BJ","WBA","CVS",
"PEP","KO","MNST","KDP","STZ","SAM","HSY","MDLZ","GIS",
"MCD","YUM","SBUX","CMG","DPZ","QSR","WEN","SHAK","PZZA","CAKE"
]

start = "2026-02-01"
end   = "2026-02-02"

BATCH_SIZE = 40
SLEEP_SEC  = 5

all_dfs = []

t0 = time.time()

batches = list(chunks(symbols, BATCH_SIZE))
print("Total batches:", len(batches))

for i, batch in enumerate(batches, 1):
    print(f"Batch {i}/{len(batches)} size={len(batch)}")

    try:
        df = yf.download(
            tickers=batch,
            start=start,
            end=end,
            interval="15m",
            group_by="ticker",
            progress=False,
            auto_adjust=False,
        )
        if df is not None and not df.empty:
            all_dfs.append(df)
    except Exception as e:
        print("Retry batch:", e)
        time.sleep(5)

    time.sleep(SLEEP_SEC)

df_final = pd.concat(all_dfs, axis=1) if all_dfs else pd.DataFrame()

t1 = time.time()
print("Download seconds:", t1 - t0)
print("Shape:", df_final.shape)

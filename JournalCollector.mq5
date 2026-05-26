//+------------------------------------------------------------------+
//|  JournalCollector.mq5  — نسخه 4.0                               |
//+------------------------------------------------------------------+
#property copyright "PriceAlert Journal"
#property version   "4.00"
#property strict

//===================================================================
input string SERVER_URL     = "https://YOUR-APP.onrender.com";
input string API_SECRET     = "";
input string FROM_DATE      = "2026.05.01";
input string TO_DATE        = "";           // خالی = امروز
input int    CANDLES_BEFORE = 30;
input int    CANDLES_AFTER  = 30;
input bool   LOG_VERBOSE    = true;

// تایم‌فریم‌های پیش‌فرض — در صورت نیاز ادیت کن
input string TF_XAUUSD      = "5m";        // طلا همیشه M5
input string TF_BTC         = "15m";       // بیت‌کوین M15
input string TF_MAJORS      = "15m";       // جفت‌های اصلی دلار M15
input string TF_OTHER       = "1h";        // بقیه H1
//===================================================================

string g_sentFile = "JC_v4_sent.txt";

// جفت‌های اصلی دلار
bool IsMajor(string sym)
  {
   string su = sym; StringToUpper(su);
   // حذف پسوند بروکر مثل _o یا .r
   string majors[] = {"EURUSD","GBPUSD","USDJPY","USDCHF","AUDUSD","NZDUSD","USDCAD"};
   for(int i=0;i<7;i++)
      if(StringFind(su, majors[i]) >= 0) return true;
   return false;
  }

bool IsXAUUSD(string sym)
  {
   string su = sym; StringToUpper(su);
   return StringFind(su,"XAU") >= 0;
  }

bool IsBTC(string sym)
  {
   string su = sym; StringToUpper(su);
   return StringFind(su,"BTC") >= 0;
  }

string GetTFForSymbol(string sym)
  {
   if(IsXAUUSD(sym)) return TF_XAUUSD;
   if(IsBTC(sym))    return TF_BTC;
   if(IsMajor(sym))  return TF_MAJORS;
   return TF_OTHER;
  }

//+------------------------------------------------------------------+
int OnInit()
  {
   Print("=== JournalCollector v4.0 ===");
   Print("FROM:", FROM_DATE, "  TO:", TO_DATE==""?"امروز":TO_DATE);
   DrawButton();
   return INIT_SUCCEEDED;
  }

void OnDeinit(const int reason)
  {
   ObjectDelete(0,"JC_SendBtn");
   ObjectDelete(0,"JC_StatusLbl");
  }

void OnTick() {}

//+------------------------------------------------------------------+
datetime ParseDate(string s)
  {
   if(s=="") return TimeCurrent();
   string p[]; int n=StringSplit(s,'.',p);
   if(n<3) return TimeCurrent();
   MqlDateTime m={}; m.year=(int)StringToInteger(p[0]);
   m.mon=(int)StringToInteger(p[1]); m.day=(int)StringToInteger(p[2]);
   return StructToTime(m);
  }

ENUM_TIMEFRAMES StrToTF(string tf)
  {
   if(tf=="1m"||tf=="M1")  return PERIOD_M1;
   if(tf=="5m"||tf=="M5")  return PERIOD_M5;
   if(tf=="15m"||tf=="M15")return PERIOD_M15;
   if(tf=="30m"||tf=="M30")return PERIOD_M30;
   if(tf=="1h"||tf=="H1")  return PERIOD_H1;
   if(tf=="4h"||tf=="H4")  return PERIOD_H4;
   if(tf=="1d"||tf=="D1")  return PERIOD_D1;
   return PERIOD_H1;
  }

string FormatTehran(datetime utc)
  {
   datetime teh=utc+3*3600+30*60;
   MqlDateTime m; TimeToStruct(teh,m);
   return StringFormat("%04d-%02d-%02d %02d:%02d:%02d",m.year,m.mon,m.day,m.hour,m.min,m.sec);
  }

string EscJ(string s)
  {
   StringReplace(s,"\\","\\\\"); StringReplace(s,"\"","\\\"");
   StringReplace(s,"\n","\\n");  StringReplace(s,"\r","\\r");
   return s;
  }

string GetOutcome(double p)
  { return p>0.01?"win":p<-0.01?"loss":"breakeven"; }

//+------------------------------------------------------------------+
string GetCandles(string sym, ENUM_TIMEFRAMES tf, datetime entry, datetime exitt)
  {
   SymbolSelect(sym,true);
   datetime from=entry-(datetime)(CANDLES_BEFORE*PeriodSeconds(tf));
   datetime to=exitt+(datetime)(CANDLES_AFTER*PeriodSeconds(tf));
   if(to>TimeCurrent()) to=TimeCurrent();
   MqlRates r[]; ArraySetAsSeries(r,false);
   int n=CopyRates(sym,tf,from,to,r);
   if(n<=0){Print("[EA] CopyRates 0 ",sym," err=",GetLastError());return "[]";}
   string a="[";
   for(int i=0;i<n;i++)
     {
      if(i>0) a+=",";
      a+=StringFormat("{\"t\":%d,\"o\":%.5f,\"h\":%.5f,\"l\":%.5f,\"c\":%.5f,\"v\":%d}",
                      (long)r[i].time,r[i].open,r[i].high,r[i].low,r[i].close,(long)r[i].tick_volume);
     }
   return a+"]";
  }

//+------------------------------------------------------------------+
bool PostJSON(string endpoint, string body, string &resp)
  {
   string url=SERVER_URL+endpoint;
   string hdrs="Content-Type: application/json\r\n";
   if(StringLen(API_SECRET)>0) hdrs+="X-API-Secret: "+API_SECRET+"\r\n";
   uchar pd[],rd[]; string rh;
   StringToCharArray(body,pd,0,StringLen(body),CP_UTF8);
   int sz=ArraySize(pd); if(sz>0&&pd[sz-1]==0) ArrayResize(pd,sz-1);
   int res=-1;
   for(int i=0;i<2&&res==-1;i++){if(i>0)Sleep(2000);ResetLastError();res=WebRequest("POST",url,hdrs,30000,pd,rd,rh);}
   if(res==-1){int e=GetLastError();Print("[EA] err:",e);if(e==4014)Print("[EA] URL اضافه کن: ",SERVER_URL);return false;}
   resp=CharArrayToString(rd,0,WHOLE_ARRAY,CP_UTF8);
   if(res!=200&&res!=201){Print("[EA] HTTP ",res,": ",StringSubstr(resp,0,120));return false;}
   if(LOG_VERBOSE) Print("[EA] OK → ",StringSubstr(resp,0,80));
   return true;
  }

bool IsSent(long pid)
  {
   int h=FileOpen(g_sentFile,FILE_READ|FILE_TXT|FILE_SHARE_READ|FILE_ANSI);
   if(h==INVALID_HANDLE) return false;
   string c=""; while(!FileIsEnding(h)) c+=FileReadString(h); FileClose(h);
   return StringFind(c,"|"+IntegerToString(pid)+"|")>=0;
  }

void MarkSent(long pid)
  {
   int h=FileOpen(g_sentFile,FILE_READ|FILE_WRITE|FILE_TXT|FILE_SHARE_READ|FILE_ANSI);
   if(h==INVALID_HANDLE) h=FileOpen(g_sentFile,FILE_WRITE|FILE_TXT|FILE_ANSI);
   if(h==INVALID_HANDLE) return;
   FileSeek(h,0,SEEK_END); FileWriteString(h,"|"+IntegerToString(pid)+"|"); FileClose(h);
  }

//+------------------------------------------------------------------+
bool SendPosition(long pos_id, int total_deals)
  {
   string sym="",direction="",comment="";
   double entry_price=0,exit_price=0,lots=0,total_profit=0,sl_price=0,tp_price=0;
   datetime entry_time=0,exit_time=0;
   ulong entry_ticket=0;
   bool found_exit=false;

   for(int i=0;i<total_deals;i++)
     {
      ulong d=HistoryDealGetTicket(i);
      if((long)HistoryDealGetInteger(d,DEAL_POSITION_ID)!=pos_id) continue;
      ENUM_DEAL_TYPE  dt=(ENUM_DEAL_TYPE) HistoryDealGetInteger(d,DEAL_TYPE);
      ENUM_DEAL_ENTRY de=(ENUM_DEAL_ENTRY)HistoryDealGetInteger(d,DEAL_ENTRY);
      if(dt!=DEAL_TYPE_BUY&&dt!=DEAL_TYPE_SELL) continue;

      if(de==DEAL_ENTRY_IN)
        {
         entry_ticket=d; sym=HistoryDealGetString(d,DEAL_SYMBOL);
         direction=(dt==DEAL_TYPE_BUY)?"BUY":"SELL";
         entry_price=HistoryDealGetDouble(d,DEAL_PRICE);
         entry_time=(datetime)HistoryDealGetInteger(d,DEAL_TIME);
         lots=HistoryDealGetDouble(d,DEAL_VOLUME);
         comment=HistoryDealGetString(d,DEAL_COMMENT);
         double ds=HistoryDealGetDouble(d,DEAL_SL),dt2=HistoryDealGetDouble(d,DEAL_TP);
         if(ds>0) sl_price=ds; if(dt2>0) tp_price=dt2;
        }
      else if(de==DEAL_ENTRY_OUT||de==DEAL_ENTRY_INOUT)
        {
         exit_price=HistoryDealGetDouble(d,DEAL_PRICE);
         exit_time=(datetime)HistoryDealGetInteger(d,DEAL_TIME);
         total_profit+=HistoryDealGetDouble(d,DEAL_PROFIT)+HistoryDealGetDouble(d,DEAL_SWAP)+HistoryDealGetDouble(d,DEAL_COMMISSION);
         double ds=HistoryDealGetDouble(d,DEAL_SL),dt2=HistoryDealGetDouble(d,DEAL_TP);
         if(ds>0&&sl_price==0) sl_price=ds; if(dt2>0&&tp_price==0) tp_price=dt2;
         found_exit=true;
        }
     }

   if(!found_exit){if(LOG_VERBOSE)Print("[EA] pos=",pos_id," باز — skip");return false;}
   if(sym==""||entry_price==0) return false;

   string tf_str=GetTFForSymbol(sym);
   ENUM_TIMEFRAMES tf=StrToTF(tf_str);
   string outcome=GetOutcome(total_profit);
   string exit_type=(outcome=="loss")?"sl":"tp";

   double mul=10000.0; string su=sym; StringToUpper(su);
   if(StringFind(su,"JPY")>=0) mul=100.0;
   if(StringFind(su,"XAU")>=0||StringFind(su,"XAG")>=0) mul=10.0;
   if(StringFind(su,"BTC")>=0||StringFind(su,"ETH")>=0) mul=1.0;

   double sl_pips=sl_price>0?MathAbs(entry_price-sl_price)*mul:0;
   double tp_pips=tp_price>0?MathAbs(tp_price-entry_price)*mul:0;
   string candles=GetCandles(sym,tf,entry_time,exit_time);

   Print("[EA] ",sym," → TF:",tf_str," SL:",sl_price," TP:",tp_price);

   string json="{";
   json+="\"sym\":\""          +EscJ(sym)+"\",";
   json+="\"tf\":\""           +tf_str+"\",";
   json+="\"direction\":\""    +direction+"\",";
   json+="\"entry\":"          +DoubleToString(entry_price,5)+",";
   json+="\"exit\":"           +DoubleToString(exit_price,5)+",";
   json+="\"sl_price\":"       +(sl_price>0?DoubleToString(sl_price,5):"null")+",";
   json+="\"tp_price\":"       +(tp_price>0?DoubleToString(tp_price,5):"null")+",";
   json+="\"sl_pips\":"        +(sl_pips>0?DoubleToString(sl_pips,1):"null")+",";
   json+="\"tp_pips\":"        +(tp_pips>0?DoubleToString(tp_pips,1):"null")+",";
   json+="\"size\":"           +DoubleToString(lots,2)+",";
   json+="\"entryTime\":\""    +FormatTehran(entry_time)+"\",";
   json+="\"exitTime\":\""     +FormatTehran(exit_time)+"\",";
   json+="\"outcome\":\""      +outcome+"\",";
   json+="\"exit_type\":\""    +exit_type+"\",";
   json+="\"pnl\":"            +DoubleToString(total_profit,2)+",";
   json+="\"mt4_ticket\":"     +IntegerToString(entry_ticket)+",";
   json+="\"mt4_position_id\":"+IntegerToString(pos_id)+",";
   json+="\"mt4_profit\":"     +DoubleToString(total_profit,2)+",";
   json+="\"note\":\""         +EscJ(comment)+"\",";
   json+="\"source\":\"mt5_ea\",";
   json+="\"candle_snapshot\":"+candles;
   json+="}";

   string resp="";
   bool ok=PostJSON("/api/journal/mt4",json,resp);
   if(ok) Print("[EA] ✅ ",sym," ",direction," ",outcome," tf=",tf_str);
   else   Print("[EA] ❌ ",sym," pos=",pos_id);
   return ok;
  }

//+------------------------------------------------------------------+
void CollectAndSend()
  {
   datetime from=ParseDate(FROM_DATE);
   datetime to=(TO_DATE=="")?TimeCurrent():ParseDate(TO_DATE)+86400;
   Print("=== ارسال از ",FROM_DATE," تا ",TO_DATE==""?"امروز":TO_DATE," ===");
   UpdateStatus("در حال ارسال...",clrYellow);

   if(!HistorySelect(from,to)){Print("[EA] HistorySelect ناموفق");UpdateStatus("خطا",clrRed);return;}

   int total_deals=HistoryDealsTotal();
   Print("[EA] deals: ",total_deals);

   long positions[]; int pos_count=0;
   for(int i=0;i<total_deals;i++)
     {
      ulong d=HistoryDealGetTicket(i);
      long pid=HistoryDealGetInteger(d,DEAL_POSITION_ID);
      if(pid==0) continue;
      bool dup=false;
      for(int x=0;x<pos_count;x++) if(positions[x]==pid){dup=true;break;}
      if(dup) continue;
      ArrayResize(positions,pos_count+1); positions[pos_count++]=pid;
     }

   Print("[EA] positions: ",pos_count);
   int sent=0,skip=0;

   for(int i=0;i<pos_count;i++)
     {
      long pid=positions[i];
      if(IsSent(pid)){skip++;continue;}
      bool ok=SendPosition(pid,total_deals);
      if(ok){MarkSent(pid);sent++;Sleep(600);}
      else skip++;
     }

   string msg=StringFormat("✅ %d ارسال | %d skip",sent,skip);
   Print("=== تمام — ",msg," ===");
   UpdateStatus(msg,sent>0?clrLime:clrSilver);
  }

//+------------------------------------------------------------------+
void DrawButton()
  {
   string b="JC_SendBtn";
   ObjectDelete(0,b); ObjectCreate(0,b,OBJ_BUTTON,0,0,0);
   ObjectSetInteger(0,b,OBJPROP_XDISTANCE,15); ObjectSetInteger(0,b,OBJPROP_YDISTANCE,30);
   ObjectSetInteger(0,b,OBJPROP_XSIZE,230);    ObjectSetInteger(0,b,OBJPROP_YSIZE,36);
   ObjectSetString(0,b,OBJPROP_TEXT,"📤  Journal  ["+FROM_DATE+" → "+(TO_DATE==""?"امروز":TO_DATE)+"]");
   ObjectSetInteger(0,b,OBJPROP_COLOR,clrWhite);
   ObjectSetInteger(0,b,OBJPROP_BGCOLOR,C'40,40,55');
   ObjectSetInteger(0,b,OBJPROP_FONTSIZE,9);
   ObjectSetInteger(0,b,OBJPROP_CORNER,CORNER_LEFT_UPPER);
   ObjectSetInteger(0,b,OBJPROP_SELECTABLE,false);

   string l="JC_StatusLbl";
   ObjectDelete(0,l); ObjectCreate(0,l,OBJ_LABEL,0,0,0);
   ObjectSetInteger(0,l,OBJPROP_XDISTANCE,15); ObjectSetInteger(0,l,OBJPROP_YDISTANCE,72);
   ObjectSetString(0,l,OBJPROP_TEXT,"آماده — دکمه رو بزن");
   ObjectSetInteger(0,l,OBJPROP_COLOR,clrSilver);
   ObjectSetInteger(0,l,OBJPROP_FONTSIZE,9);
   ObjectSetInteger(0,l,OBJPROP_CORNER,CORNER_LEFT_UPPER);
   ObjectSetInteger(0,l,OBJPROP_SELECTABLE,false);
   ChartRedraw(0);
  }

void UpdateStatus(string msg,color clr)
  {ObjectSetString(0,"JC_StatusLbl",OBJPROP_TEXT,msg);ObjectSetInteger(0,"JC_StatusLbl",OBJPROP_COLOR,clr);ChartRedraw(0);}

void OnChartEvent(const int id,const long &lp,const double &dp,const string &sp)
  {
   if(id==CHARTEVENT_OBJECT_CLICK&&sp=="JC_SendBtn")
     {ObjectSetInteger(0,"JC_SendBtn",OBJPROP_STATE,false);CollectAndSend();}
  }
//+------------------------------------------------------------------+

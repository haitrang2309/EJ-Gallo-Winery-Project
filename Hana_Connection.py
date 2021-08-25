# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 15:41:31 2021

@author: ijr
"""

#pip install dbapi
# pip install hdbcli
from hdbcli import dbapi
import pandas as pd

def main():
        conn = dbapi.connect(
           address="galdwq1db01.ejgallo.com",
           port=32015,
           user="IRAMIRE5",
           password="Gallo@123"
        )
        
        nopa_query = 'SELECT "SAMPLE_SUMMARY".SAMPLE_ID, "SAMPLE_SUMMARY".LOT, "SAMPLE_SUMMARY".S_CREATED_DATE,"SAMPLE_SUMMARY".EQUIPMENT_ID, "PARAM_DETAIL".PL_ID, "PARAM_DETAIL".P_VALUE\
         FROM _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_SAMPLE_SUMMARY" AS "SAMPLE_SUMMARY"\
         INNER JOIN _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_PARAM_DETAIL" AS "PARAM_DETAIL" ON\
         ("SAMPLE_SUMMARY".SAMPLE_ID = "PARAM_DETAIL".SAMPLE_ID)\
         INNER JOIN _SYS_BIC."EDW.MasterData.WMGViews/MD_WMGMT11_WN_TYP" AS "WINE_TYP" ON\
         (SUBSTRING("SAMPLE_SUMMARY".LOT, 1, 9) = "WINE_TYP".WN_TYP_I)\
         WHERE SUBSTRING("SAMPLE_SUMMARY".LOCATION_PATH, 1, 3) = \'LVW\'\
         AND "SAMPLE_SUMMARY".PROCESS_STATE = \'Fermentation - Primary\'\
         AND "PARAM_DETAIL".VIEWABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".REPORTABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".P_RELEASED_FLAG = \'Y\'\
         AND "PARAM_DETAIL".PL_ID = \'NOPA\''
        
        ammonia_query = 'SELECT "SAMPLE_SUMMARY".SAMPLE_ID, "SAMPLE_SUMMARY".LOT, "SAMPLE_SUMMARY".S_CREATED_DATE,"SAMPLE_SUMMARY".EQUIPMENT_ID, "PARAM_DETAIL".PL_ID, "PARAM_DETAIL".P_VALUE\
         FROM _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_SAMPLE_SUMMARY" AS "SAMPLE_SUMMARY"\
         INNER JOIN _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_PARAM_DETAIL" AS "PARAM_DETAIL" ON\
         ("SAMPLE_SUMMARY".SAMPLE_ID = "PARAM_DETAIL".SAMPLE_ID)\
         INNER JOIN _SYS_BIC."EDW.MasterData.WMGViews/MD_WMGMT11_WN_TYP" AS "WINE_TYP" ON\
         (SUBSTRING("SAMPLE_SUMMARY".LOT, 1, 9) = "WINE_TYP".WN_TYP_I)\
         WHERE SUBSTRING("SAMPLE_SUMMARY".LOCATION_PATH, 1, 3) = \'LVW\'\
         AND "SAMPLE_SUMMARY".PROCESS_STATE = \'Fermentation - Primary\'\
         AND "PARAM_DETAIL".VIEWABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".REPORTABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".P_RELEASED_FLAG = \'Y\'\
         AND "PARAM_DETAIL".PL_ID = \'Ammonia\''
        
        glucose_query = 'SELECT "SAMPLE_SUMMARY".SAMPLE_ID, "SAMPLE_SUMMARY".LOT, "SAMPLE_SUMMARY".S_CREATED_DATE,"SAMPLE_SUMMARY".EQUIPMENT_ID, "PARAM_DETAIL".PL_ID, "PARAM_DETAIL".P_VALUE\
         FROM _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_SAMPLE_SUMMARY" AS "SAMPLE_SUMMARY"\
         INNER JOIN _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_PARAM_DETAIL" AS "PARAM_DETAIL" ON\
         ("SAMPLE_SUMMARY".SAMPLE_ID = "PARAM_DETAIL".SAMPLE_ID)\
         INNER JOIN _SYS_BIC."EDW.MasterData.WMGViews/MD_WMGMT11_WN_TYP" AS "WINE_TYP" ON\
         (SUBSTRING("SAMPLE_SUMMARY".LOT, 1, 9) = "WINE_TYP".WN_TYP_I)\
         WHERE SUBSTRING("SAMPLE_SUMMARY".LOCATION_PATH, 1, 3) = \'LVW\'\
         AND "SAMPLE_SUMMARY".PROCESS_STATE = \'Fermentation - Primary\'\
         AND "PARAM_DETAIL".VIEWABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".REPORTABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".P_RELEASED_FLAG = \'Y\'\
         AND "PARAM_DETAIL".PL_ID = \'Glucose/Fructose\''
        
        ph_query = 'SELECT "SAMPLE_SUMMARY".SAMPLE_ID, "SAMPLE_SUMMARY".LOT, "SAMPLE_SUMMARY".S_CREATED_DATE,"SAMPLE_SUMMARY".EQUIPMENT_ID,"PARAM_DETAIL".PL_ID, "PARAM_DETAIL".P_VALUE\
         FROM _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_SAMPLE_SUMMARY" AS "SAMPLE_SUMMARY"\
         INNER JOIN _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_PARAM_DETAIL" AS "PARAM_DETAIL" ON\
         ("SAMPLE_SUMMARY".SAMPLE_ID = "PARAM_DETAIL".SAMPLE_ID)\
         INNER JOIN _SYS_BIC."EDW.MasterData.WMGViews/MD_WMGMT11_WN_TYP" AS "WINE_TYP" ON\
         (SUBSTRING("SAMPLE_SUMMARY".LOT, 1, 9) = "WINE_TYP".WN_TYP_I)\
         WHERE SUBSTRING("SAMPLE_SUMMARY".LOCATION_PATH, 1, 3) = \'LVW\'\
         AND "SAMPLE_SUMMARY".PROCESS_STATE = \'Fermentation - Primary\'\
         AND "PARAM_DETAIL".VIEWABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".REPORTABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".P_RELEASED_FLAG = \'Y\'\
         AND "PARAM_DETAIL".PL_ID = \'pH\''
         
        total_so2_query = 'SELECT "SAMPLE_SUMMARY".SAMPLE_ID, "SAMPLE_SUMMARY".LOT, "SAMPLE_SUMMARY".S_CREATED_DATE,"SAMPLE_SUMMARY".EQUIPMENT_ID,"PARAM_DETAIL".PL_ID, "PARAM_DETAIL".P_VALUE\
         FROM _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_SAMPLE_SUMMARY" AS "SAMPLE_SUMMARY"\
         INNER JOIN _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_PARAM_DETAIL" AS "PARAM_DETAIL" ON\
         ("SAMPLE_SUMMARY".SAMPLE_ID = "PARAM_DETAIL".SAMPLE_ID)\
         INNER JOIN _SYS_BIC."EDW.MasterData.WMGViews/MD_WMGMT11_WN_TYP" AS "WINE_TYP" ON\
         (SUBSTRING("SAMPLE_SUMMARY".LOT, 1, 9) = "WINE_TYP".WN_TYP_I)\
         WHERE SUBSTRING("SAMPLE_SUMMARY".LOCATION_PATH, 1, 3) = \'LVW\'\
         AND "SAMPLE_SUMMARY".PROCESS_STATE = \'Fermentation - Primary\'\
         AND "PARAM_DETAIL".VIEWABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".REPORTABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".P_RELEASED_FLAG = \'Y\'\
         AND "PARAM_DETAIL".PL_ID = \'Total SO2\''

        free_so2_query = 'SELECT "SAMPLE_SUMMARY".SAMPLE_ID, "SAMPLE_SUMMARY".LOT, "SAMPLE_SUMMARY".S_CREATED_DATE,"SAMPLE_SUMMARY".EQUIPMENT_ID,"PARAM_DETAIL".PL_ID, "PARAM_DETAIL".P_VALUE\
         FROM _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_SAMPLE_SUMMARY" AS "SAMPLE_SUMMARY"\
         INNER JOIN _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_PARAM_DETAIL" AS "PARAM_DETAIL" ON\
         ("SAMPLE_SUMMARY".SAMPLE_ID = "PARAM_DETAIL".SAMPLE_ID)\
         INNER JOIN _SYS_BIC."EDW.MasterData.WMGViews/MD_WMGMT11_WN_TYP" AS "WINE_TYP" ON\
         (SUBSTRING("SAMPLE_SUMMARY".LOT, 1, 9) = "WINE_TYP".WN_TYP_I)\
         WHERE SUBSTRING("SAMPLE_SUMMARY".LOCATION_PATH, 1, 3) = \'LVW\'\
         AND "SAMPLE_SUMMARY".PROCESS_STATE = \'Fermentation - Primary\'\
         AND "PARAM_DETAIL".VIEWABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".REPORTABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".P_RELEASED_FLAG = \'Y\'\
         AND "PARAM_DETAIL".PL_ID = \'Free SO2\''         

        lactic_acid_query = 'SELECT "SAMPLE_SUMMARY".SAMPLE_ID, "SAMPLE_SUMMARY".LOT, "SAMPLE_SUMMARY".S_CREATED_DATE,"SAMPLE_SUMMARY".EQUIPMENT_ID,"PARAM_DETAIL".PL_ID, "PARAM_DETAIL".P_VALUE\
         FROM _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_SAMPLE_SUMMARY" AS "SAMPLE_SUMMARY"\
         INNER JOIN _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_PARAM_DETAIL" AS "PARAM_DETAIL" ON\
         ("SAMPLE_SUMMARY".SAMPLE_ID = "PARAM_DETAIL".SAMPLE_ID)\
         INNER JOIN _SYS_BIC."EDW.MasterData.WMGViews/MD_WMGMT11_WN_TYP" AS "WINE_TYP" ON\
         (SUBSTRING("SAMPLE_SUMMARY".LOT, 1, 9) = "WINE_TYP".WN_TYP_I)\
         WHERE SUBSTRING("SAMPLE_SUMMARY".LOCATION_PATH, 1, 3) = \'LVW\'\
         AND "SAMPLE_SUMMARY".PROCESS_STATE = \'Fermentation - Primary\'\
         AND "PARAM_DETAIL".VIEWABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".REPORTABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".P_RELEASED_FLAG = \'Y\'\
         AND "PARAM_DETAIL".PL_ID = \'Lactic Acid\''            

        malic_acid_query = 'SELECT "SAMPLE_SUMMARY".SAMPLE_ID, "SAMPLE_SUMMARY".LOT, "SAMPLE_SUMMARY".S_CREATED_DATE,"SAMPLE_SUMMARY".EQUIPMENT_ID,"PARAM_DETAIL".PL_ID, "PARAM_DETAIL".P_VALUE\
         FROM _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_SAMPLE_SUMMARY" AS "SAMPLE_SUMMARY"\
         INNER JOIN _SYS_BIC."EDW.LIMSViews.Semantic/LIMSLV_RPT_PARAM_DETAIL" AS "PARAM_DETAIL" ON\
         ("SAMPLE_SUMMARY".SAMPLE_ID = "PARAM_DETAIL".SAMPLE_ID)\
         INNER JOIN _SYS_BIC."EDW.MasterData.WMGViews/MD_WMGMT11_WN_TYP" AS "WINE_TYP" ON\
         (SUBSTRING("SAMPLE_SUMMARY".LOT, 1, 9) = "WINE_TYP".WN_TYP_I)\
         WHERE SUBSTRING("SAMPLE_SUMMARY".LOCATION_PATH, 1, 3) = \'LVW\'\
         AND "SAMPLE_SUMMARY".PROCESS_STATE = \'Fermentation - Primary\'\
         AND "PARAM_DETAIL".VIEWABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".REPORTABLE_IND = \'Y\'\
         AND "PARAM_DETAIL".P_RELEASED_FLAG = \'Y\'\
         AND "PARAM_DETAIL".PL_ID = \'Malic Acid\''          
         
         
        #load ingredience from HANA
        ingred_query ='SELECT _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST"."TNK_I",\
         _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST"."WRK_ORD_I",\
         _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST"."CMPLT_T",\
         _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST"."LOT_I",\
         _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST"."WRK_ORD_SYS_I",\
         _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST"."CURR_LOT_SYS_I",\
         _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST"."INGRED_CMPST_SYS_I",\
         _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST"."SITE_D",\
         TO_CHAR(TRIM(LEADING 0 FROM _SYS_BIC."EDW.MasterData.MasterDimension.Material/MD_Material"."Material")),\
         _SYS_BIC."EDW.MasterData.MasterDimension.Material/MD_Material"."MaterialName",\
         _SYS_BIC."EDW.MasterData.MasterDimension.Material/MD_Material"."MaterialType",\
         _SYS_BIC."EDW.MasterData.MasterDimension.Material/MD_Material"."MaterialTypeName",\
         _SYS_BIC."EDW.MasterData.MasterDimension.Material/MD_Material"."MaterialGroupName",\
         _SYS_BIC."EDW.MasterData.MasterDimension.Material/MD_Material"."MaterialWeightUnit", \
         _SYS_BIC."EDW.MasterData.MasterDimension.Material/MD_Material"."MaterialNetWeight",\
         _SYS_BIC."EDW.MasterData.MasterDimension.Material/MD_Material"."MaterialGrossWeight",\
         _SYS_BIC."EDW.MasterData.WMGViews/MD_WMG_INGREDIENT_CMPST"."INGRED_I",\
         _SYS_BIC."EDW.MasterData.WMGViews/MD_WMG_INGREDIENT_CMPST"."CMPST_SYS_I",\
         _SYS_BIC."EDW.MasterData.WMGViews/MD_WMG_INGREDIENT_CMPST"."ADD_T",\
         _SYS_BIC."EDW.MasterData.WMGViews/MD_WMG_INGREDIENT_CMPST"."CREATE_T",\
        _SYS_BIC."EDW.MasterData.WMGViews/MD_WMG_INGREDIENT_CMPST"."LAST_UPDATE_T"\
         FROM _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST"\
         JOIN _SYS_BIC."EDW.MasterData.WMGViews/MD_WMG_INGREDIENT_CMPST"\
         ON _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST"."INGRED_CMPST_SYS_I" = _SYS_BIC."EDW.MasterData.WMGViews/MD_WMG_INGREDIENT_CMPST"."CMPST_SYS_I"\
         JOIN _SYS_BIC."EDW.MasterData.MasterDimension.Material/MD_Material"\
         ON _SYS_BIC."EDW.MasterData.MasterDimension.Material/MD_Material"."Material" = _SYS_BIC."EDW.MasterData.WMGViews/MD_WMG_INGREDIENT_CMPST"."INGRED_I"\
         WHERE _SYS_BIC."EDW.WMGViews.Foundation/WMG_FND_HALFLEG_CMPST".SITE_D = \'LIVINGSTON WINERY\' LIMIT 100'
        
        
        df_nopa= pd.read_sql_query(nopa_query, conn)
        df_ammonia = pd.read_sql_query(ammonia_query, conn) 
        df_glucose = pd.read_sql_query(glucose_query, conn) 
        df_ph = pd.read_sql_query(ph_query, conn) 
        df_totalso2 = pd.read_sql_query(total_so2_query, conn) 
        df_freeso2 = pd.read_sql_query(free_so2_query, conn) 
        df_lactic = pd.read_sql_query(lactic_acid_query, conn) 
        df_malic = pd.read_sql_query(malic_acid_query, conn) 
        df_ingred = pd.read_sql_query(ingred_query, conn)
        
        return df_nopa, df_ammonia,df_glucose, df_ph, df_totalso2,df_freeso2, df_lactic, df_malic, df_ingred

if __name__ == "__main__":
    main()
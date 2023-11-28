package sttDB.util;


import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import sttDB.SttDB;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DBUtil extends SttDB {
    private static final Logger log = (Logger) LoggerFactory.getLogger(DBUtil.class);

    //private final Logger log  = super.loginfo();

    // DB fetch건수
    private final int FETCH_SIZE = 1000;

    /**
     * STT관리 데이터등록
     * @param parms DB등록 데이터
     * @param con DB연결
     * @return 성공여부
     */
    public boolean insertSttMgmt(Map<String, Object> parms , Connection con) {

        log.debug("[DBUtil](insertSttMgmt) parms : " + parms );
        log.info("???????TARGET_SERVER?" + TARGET_SERVER);
        PreparedStatement prprdsttmn = null;
        String query = null;
        try {
            String sql = "";
            sql += "INSERT INTO /* DBUtil insertSttMgmt */                      ";
            sql += "            STT_MGMT /* STT관리 */						     ";
            sql += "          ( STT_ID            /* STT번호 */				     ";
            sql += "          , VOC_CNSL_ID       /* 상담번호 */				     ";
            sql += "          , TICKET_ID         /* 티켓번호 */				     ";
            sql += "          , CTI_CALL_ID       /* CTI콜 ID */				 ";
            sql += "          , INTRN_LINE_NUM    /* 내선번호 */				     ";
            sql += "          , CUST_TEL_NUM      /* 고객전화번호 */			     ";
            sql += "          , IMPRV_YN          /* 개선여부 */				     ";
            sql += "          , IMPRV_RQST_DATE   /* 개선요청일시 */			     ";
            sql += "          , STT_CNTNT         /* STT_컨텐츠 */			     ";
            sql += "          , STT_FILE_NAME     /* STT파일명 */				 ";
            sql += "          , STT_FILE_PATH     /* STT파일경로 */			     ";
            sql += "          , STT_FILE_DEL_DATE /* STT파일삭제일시 */		     ";
            sql += "          , TEL_STRT_DT       /* 전화시작일자 */			     ";
            sql += "          , TEL_STRT_TM       /* 전화시작시간 */			     ";
            sql += "          , REGDATE           /* 등록일시 */				     ";
            sql += "          , REGUSER           /* 등록직원 */				     ";
            sql += "          , EDTDATE           /* 수정일시 */				     ";
            sql += "          , EDTUSER           /* 수정직원 */				     ";
            sql += "          , IMPRV_TYPE_CD     /* 개선유형코드 */			     ";
            sql += "          )												     ";
            sql += "SELECT      ?       AS STT_ID            /* [1]STT번호 */	 ";
            sql += "          , NULL    AS VOC_CNSL_ID       /* 상담번호 */	     ";
            sql += "          , NULL    AS TICKET_ID         /* 티켓번호 */	     ";
            sql += "          , NULL    AS CTI_CALL_ID       /* CTI콜 ID */	     ";
            sql += "          , ?       AS INTRN_LINE_NUM    /* [2]내선번호 */	 ";
            sql += "          , ?       AS CUST_TEL_NUM      /* [3]고객전화번호 */ ";
            sql += "          , NULL    AS IMPRV_YN          /* 개선여부 */	     ";
            sql += "          , NULL    AS IMPRV_RQST_DATE   /* 개선요청일시 */     ";
            sql += "          , NULL    AS STT_CNTNT         /* STT_컨텐츠 */	 ";
            sql += "          , NULL    AS STT_FILE_NAME     /* STT파일명 */	     ";
            sql += "          , NULL    AS STT_FILE_PATH     /* STT파일경로 */     ";
            sql += "          , NULL    AS STT_FILE_DEL_DATE /* STT파일삭제일시 */  ";
            sql += "          , ?       AS TEL_STRT_DT       /* [4]전화시작일자 */     ";
            sql += "          , ?       AS TEL_STRT_TM       /* [5]전화시작시간 */     ";
            sql += "          , SYSDATE AS REGDATE           /* 등록일시 */	     ";
            sql += "          , 'STT"+TARGET_SERVER+"'   AS REGUSER           /* 등록직원 */	     ";
            sql += "          , SYSDATE AS EDTDATE           /* 수정일시 */	     ";
            sql += "          , 'STT"+TARGET_SERVER+"'   AS EDTUSER           /* 수정직원 */	     ";
            sql += "          , NULL    AS IMPRV_TYPE_CD     /* 개선유형코드 */     ";
            sql += "FROM DUAL												     ";
            sql += "WHERE NOT EXISTS ( SELECT 1								     ";
            sql += "                   FROM   STT_MGMT 						     ";
            sql += "				   WHERE  STT_ID = ? /* [6]STT번호 */ )		 ";
            query = sql;
            prprdsttmn = con.prepareStatement(query);
            prprdsttmn.setString(1, (String) parms.get("sttId"));       // [1]STT번호
            prprdsttmn.setString(2, (String) parms.get("intrnLineNum"));// [2]내선번호
            prprdsttmn.setString(3, (String) parms.get("custTelNum"));  // [3]고객전화번호
            prprdsttmn.setString(4, (String) parms.get("telStrtDt"));   // [4]발화시작일자
            prprdsttmn.setString(5, (String) parms.get("telStrtTm"));   // [5]발화시작일자
            prprdsttmn.setString(6, (String) parms.get("sttId"));       // [6]STT번호
            int rows = prprdsttmn.executeUpdate();
            log.debug("sql?" + query);
            log.info("[DBUtil](insertSttMgmt) insert rows?" + rows);
            prprdsttmn.close();

        } catch (SQLException e){
            log.error("[DBUtil](insertSttMgmt) SQLException" + query + e);
            return false;
        } catch (Exception e) {
            log.error("[DBUtil](insertSttMgmt) Exception" + e);
            return false;
        } finally {
            if ( prprdsttmn != null) {
                try {
                    prprdsttmn.close();
                } catch (SQLException e) {
                    log.error("[DBUtil](insertSttMgmt) statement cannot be closed." + e);
                    return false;
                }
            }
        }
        return true;
    }


    /**
     * STT관리상세 데이터등록
     * @param parms
     * @param con
     * @return
     */
    public boolean insertSttMgmtDtl(Map<String, Object> parms , Connection con)  {

        //log.debug("[DBUtil](insertSttMgmtDtl) parms : " + parms + ", driverName : ");
        PreparedStatement prprdsttmn = null;
        String query = null;

        try {

            if(parms.get("sttList") == null){
                log.error("sttList is null");
                return false;
            }

            String sql ="";
            sql += "INSERT INTO /* insertSttMgmtDtl */                ";
            sql += "            STT_MGMT_DTL /* STT관리상세 */	";
            sql += "          ( STT_ID        /* STT번호 */		";
            sql += "          , STT_SEQ       /* STT일련번호 */	";
            sql += "          , SPKR_SPRTN_CD /* 화자분리코드 */	";
            sql += "          , STT_CNTNT     /* STT_컨텐츠 */	";
            sql += "          , ARTCL_STRT_DT /* 발화시작일자 */	";
            sql += "          , ARTCL_STRT_TM /* 발화시작시간 */	";
            sql += "          , ARTCL_END_TM  /* 발화종료시간 */	";
            sql += "          , REGDATE       /* 등록일시 */		";
            sql += "          , REGUSER       /* 등록직원 */		";
            sql += "          , EDTDATE       /* 수정일시 */		";
            sql += "          , EDTUSER       /* 수정직원 */		";
            sql += "          )				                 	";

            // select
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> list = (List<Map<String, Object>>) parms.get("sttList");
            List<Map<String, Object>> listSub;
            int idx = 0;

            StringBuilder subSql;
            int subIdx = 0;
            for (int i = 0; i < list.size(); i += FETCH_SIZE) {
                //log.debug("/////////////////////////////////////////// i:" + i);
                int endIndex = Math.min(i + FETCH_SIZE, list.size());
                listSub = new ArrayList<>(list.subList(i, endIndex));
                subIdx = 0; // 초기화
                subSql = new StringBuilder();
                for (Map<String, Object> data : listSub) {
                    long currentTimeMillis = System.currentTimeMillis();
                    subSql.append("SELECT  '").append(parms.get("sttId")).append("' AS STT_ID        /* STT번호 */        ");
                    subSql.append("      , '").append(currentTimeMillis).append(idx).append("' AS STT_SEQ       /* STT일련번호 */     ");
                    subSql.append("      , '").append(parms.get("spkrSprtnCd")).append("' AS SPKR_SPRTN_CD /* 화자분리코드 */     ");
                    subSql.append("      , '").append(data.get("sttCntnt")).append("' AS STT_CNTNT     /* STT_컨텐츠 */      ");
                    subSql.append("      , '").append(parms.get("telStrtDt")).append("' AS ARTCL_STRT_DT /* 발화시작일자 */     ");
                    subSql.append("      , '").append(data.get("artclStrtTm")).append("' AS ARTCL_STRT_TM /* 발화시작시간 */     ");
                    subSql.append("      , '").append(data.get("artclEndTm")).append("' AS ARTCL_END_TM  /* 발화종료시간 */     ");
                    subSql.append("      , SYSDATE                            AS REGDATE       /* 등록일시 */        ");
                    subSql.append("      , 'STT").append(TARGET_SERVER).append("'                              AS REGUSER       /* 등록직원 */        ");
                    subSql.append("      , SYSDATE                            AS EDTDATE       /* 수정일시 */        ");
                    subSql.append("      , 'STT").append(TARGET_SERVER).append("'                              AS EDTUSER       /* 수정직원 */        ");
                    subSql.append("FROM  DUAL					");
                    idx ++;
                    subIdx ++ ;
                    if(subIdx != listSub.size() ){
                        subSql.append(" UNION ALL \n");
                    }
                }// end listSub for
                query = sql+subSql;
                log.debug("sql?" + query);
                prprdsttmn = con.prepareStatement(query);
                int rows = prprdsttmn.executeUpdate();
                log.info("[DBUtil](insertSttMgmtDtl) insert rows?" + rows);

            }// end list for


        } catch (SQLException e){
            log.error("[DBUtil](insertSttMgmtDtl) SQLException" + query + e);
            return false;
        } catch (Exception e) {
            log.error("[DBUtil](insertSttMgmtDtl) Exception" + e);
            return false;
        } finally {
            if ( prprdsttmn != null) {
                try {
                    prprdsttmn.close();
                } catch (SQLException e) {
                    log.error("[DBUtil](insertSttMgmtDtl) statement cannot be closed." + e);
                    return false;
                }
            }
        }
        return true;
    }



    /**
     * STT키워드 등록
     * @param parms
     * @param con
     * @return
     */
    public boolean insertSttKywrd(Map<String, Object> parms , Connection con)  {

        log.debug("[DBUtil](insertSttKywrd) parms : " + parms + ", driverName : ");
        PreparedStatement prprdsttmn = null;
        String query = null;
        try {
            if(parms.get("keywords") == null){
                log.info("[DBUtil](insertSttKywrd)keywords is null");
                return false;
            }

            String sql ="";
            sql += "INSERT INTO   /* insertSttKywrd */                      ";
            sql += "            STT_KYWRD /* STT키워드 */                    ";
            sql += "          ( STT_ID             /* STT번호 */             ";
            sql += "          , SPKR_SPRTN_CD      /* 화자분리코드 */         ";
            sql += "          , STT_KYWRD_SEQ      /* STT키워드순번 */        ";
            sql += "          , STT_KYWRD_CTGRY_CD /* STT키워드카테고리코드 */  ";
            sql += "          , STT_KYWRD_CTGRY_NM /* STT키워드카테고리명 */    ";
            sql += "          , STT_KYWRD_ID       /* STT키워드ID */        ";
            sql += "          , STT_KYWRD_NM       /* STT키워드명 */          ";
            sql += "          , TEL_STRT_DT        /* 전화시작일자 */         ";
            sql += "          , ARTCL_STRT_TM      /* 발화시작시간 */         ";
            sql += "          , ARTCL_END_TM       /* 발화종료시간 */         ";
            sql += "          , REGDATE            /* 등록일시 */            ";
            sql += "          , REGUSER            /* 등록직원 */            ";
            sql += "          , EDTDATE            /* 수정일시 */            ";
            sql += "          , EDTUSER            /* 수정직원 */            ";
            sql += "          )                                            ";

            // select
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> list = (List<Map<String, Object>>) parms.get("keywords");
            List<Map<String, Object>> listSub = new ArrayList<>();
            int idx = 0;

            String subSql;
            int subIdx = 0;
            for (int i = 0; i < list.size(); i += FETCH_SIZE) {
                //log.debug("STT_KYWRD /////////////////////////////////////////// i:" + i);
                int endIndex = Math.min(i + FETCH_SIZE, list.size());
                listSub = new ArrayList<>(list.subList(i, endIndex));
                subIdx = 0; // 초기화
                subSql = "";
                for (Map<String, Object> data : listSub) {
                    long currentTimeMillis = System.currentTimeMillis();
                    subSql += "/*                                                                                                ";
                    subSql += "키워드괸리에서 중복으로 등록되는 경우                                                                      ";
                    subSql += "STT키워드에서도 동일하게 등록                                                                           ";
                    subSql += "*/                                                                                                ";
                    subSql += "SELECT  '" + parms.get("sttId")              + "' AS STT_ID               /* STT번호 */            ";
                    subSql += "      , '" + parms.get("spkrSprtnCd")        + "' AS SPKR_SPRTN_CD        /* 화자분리코드 */         ";
                    subSql += "      , '" + currentTimeMillis + idx         + "' || ROWNUM AS STT_KYWRD_SEQ        /* STT키워드순번 */       ";
                    subSql += "      , M.STT_KYWRD_CTGRY_CD                      AS STT_KYWRD_CTGRY_CD   /* STT키워드카테고리코드 */ ";
                    subSql += "      , (SELECT L.COMM_NM FROM COMM_CD L WHERE M.STT_KYWRD_CTGRY_CD = L.COMM_CD AND L.COMM_CTG_ID = 'STT_KYWRD_CTGRY_CD' AND L.USE_YN = 'Y') AS STT_KYWRD_CTGRY_NM   /* STT키워드카테고리명 */  ";
                    subSql += "      , M.STT_KYWRD_ID                            AS STT_KYWRD_ID         /* STT키워드코드 */       ";
                    subSql += "      , M.STT_KYWRD_NM                            AS STT_KYWRD_NM         /* STT키워드명 */         ";
                    subSql += "      , '" + parms.get("telStrtDt")          + "' AS ARTCL_STRT_DT        /* 전화시작일자 */         ";
                    subSql += "      , '" + data.get("artclStrtTm")         + "' AS ARTCL_STRT_TM        /* 발화시작시간 */         ";
                    subSql += "      , '" + data.get("artclEndTm")          + "' AS ARTCL_END_TM         /* 발화종료시간 */         ";
                    subSql += "      , SYSDATE                                   AS REGDATE              /* 등록일시 */            ";
                    subSql += "      , 'STT"+TARGET_SERVER+"'   AS REGUSER           /* 등록직원 */  ";
                    subSql += "      , SYSDATE                                   AS EDTDATE              /* 수정일시 */            ";
                    subSql += "      , 'STT"+TARGET_SERVER+"'   AS EDTUSER           /* 수정직원 */	     ";;
                    subSql += "FROM  STT_KYWRD_MGMT M                                                                            ";
                    subSql += "WHERE M.STT_KYWRD_NM = '" + data.get("sttKywrdNm") + "'                                           ";
                    subSql += "AND   M.USE_YN = 'Y'                                                                              ";
                    subSql += "UNION                                                                                          ";
                    subSql += "/*                                                                                                ";
                    subSql += "키워드가 STT엔진과 DB와 싱크가 맞지 않는 경우                                                             ";
                    subSql += "이력으로 데이터적재                                                                                   ";
                    subSql += "향후, 데이터가 존재하면 STT엔진에 해당 키워드를 등록해야함.                                                  ";
                    subSql += "*/                                                                                               ";
                    subSql += "SELECT  '" + parms.get("sttId")              + "' AS STT_ID               /* STT번호 */           ";
                    subSql += "      , '" + parms.get("spkrSprtnCd")        + "' AS SPKR_SPRTN_CD        /* 화자분리코드 */        ";
                    subSql += "      , '" + currentTimeMillis + idx         + "' AS STT_KYWRD_SEQ        /* STT키워드순번 */      ";
                    subSql += "      , (SELECT COMM_CD FROM COMM_CD WHERE COMM_CTG_ID = 'STT_KYWRD_CTGRY_CD'  AND COMM_NM = '" + data.get("sttKywrdCtgryNm") + "' )  AS STT_KYWRD_CTGRY_CD  /* STT키워드카테고리코드 */  ";
                    subSql += "      , '" + data.get("sttKywrdCtgryNm")     + "' AS STT_KYWRD_CTGRY_NM   /* STT키워드카테고리명 */  ";
                    subSql += "      , NULL                                      AS STT_KYWRD_ID         /* STT키워드코드 */      ";
                    subSql += "      , '" + data.get("sttKywrdNm")          + "' AS STT_KYWRD_NM         /* STT키워드명 */        ";
                    subSql += "      , '" + parms.get("telStrtDt")          + "' AS ARTCL_STRT_DT        /* 전화시작일자 */        ";
                    subSql += "      , '" + data.get("artclStrtTm")         + "' AS ARTCL_STRT_TM        /* 발화시작시간 */        ";
                    subSql += "      , '" + data.get("artclEndTm")          + "' AS ARTCL_END_TM         /* 발화종료시간 */        ";
                    subSql += "      , SYSDATE AS REGDATE           /* 등록일시 */	     ";
                    subSql += "      , 'STT"+TARGET_SERVER+"'   AS REGUSER           /* 등록직원 */  ";
                    subSql += "      , SYSDATE                                   AS EDTDATE              /* 수정일시 */           ";
                    subSql += "      , 'STT"+TARGET_SERVER+"'   AS EDTUSER           /* 수정직원 */	     ";
                    subSql += "FROM  DUAL                                                                                       ";
                    subSql += "WHERE NOT EXISTS ( SELECT 1 FROM STT_KYWRD_MGMT WHERE USE_YN = 'Y' AND STT_KYWRD_NM = '" + data.get("sttKywrdNm") + "' )    ";
                    idx ++;
                    subIdx ++ ;
                    if(subIdx != listSub.size() ){
                        subSql += " UNION ALL ";
                    }


                }// end listSub for
                query = sql+subSql;
                log.debug("insertSttKywrd sql?" + query);
                prprdsttmn = con.prepareStatement(query);

                int rows = prprdsttmn.executeUpdate();
                log.info("[DBUtil](insertSttKywrd) insert rows?" + rows);

            }// end list for


        } catch (SQLException e){
            log.error("[DBUtil](insertSttKywrd) SQLException" + query + e);
            return false;
        } catch (Exception e) {
            log.error("[DBUtil](insertSttKywrd) Exception" + e);
            return false;
        } finally {
            if ( prprdsttmn != null) {
                try {
                    prprdsttmn.close();
                } catch (SQLException e) {
                    log.error("[DBUtil](insertSttKywrd) statement cannot be closed." + e);
                    return false;
                }
            }
        }
        return true;
    }

}

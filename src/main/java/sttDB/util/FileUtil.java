package sttDB.util;


import ch.qos.logback.classic.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.slf4j.LoggerFactory;
import sttDB.SttDB;

import java.io.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileUtil extends SttDB{

    private static final Logger log = (Logger) LoggerFactory.getLogger(FileUtil.class);

  //private final Logger log  = super.loginfo();
    public Map<String, Object> readFile (String path) {

        log.info("[FileUtil](readFile) path : " + path );
        Map<String, Object> resultMap = new HashMap<>();
        File file = new File(path);
        String fileNm = file.getName();
        log.info("[FileUtil](readFile) file : " + file.getName() );

        /*
         * [STEP01] 파일내용 추출
         */
        // 파일 입력스트림 생성
        FileReader fileReader;
        StringBuilder fileTxt = new StringBuilder();
        try {
            fileReader = new FileReader(file);

            // 입력 버퍼 생성
            try (BufferedReader bufferedReader = new BufferedReader(fileReader)) {

                // 읽기 수행
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    // 파일 내 문자열을 1줄씩 읽기 while
                    fileTxt.append(line).append("\n"); // 한줄씩 읽어 결과에 추가
                }
            }
            log.debug("[readFile]file text?" + fileTxt);

        } catch (FileNotFoundException e) {
            log.error("[FileNotFoundException]" +  e);
            resultMap.put("isSuccess", false);
            resultMap.put("msg", "파일을 찾을수 없습니다." + e.getMessage());
            return resultMap;
        } catch (IOException e) {
            log.error("[IOException]", e);
            resultMap.put("isSuccess", false);
            resultMap.put("msg", "STT 파일을 읽기 중 오류가 발생 되었습니다." + e.getMessage());
            return resultMap;
        }

        /*
         * [STEP02] 결과전송 json
         */
        JSONParser parser = new JSONParser();
        try {

            JSONObject jsonObject = (JSONObject) parser.parse(fileTxt.toString());

            //--------------------
            // 화자분리코드
            //--------------------
            // "RXTX":"R"
            String rxtx = (String) jsonObject.get("RXTX");
            String spkrSprtnCd;
            if(rxtx.endsWith("R") || rxtx.endsWith("T")){
                spkrSprtnCd = rxtx + "X";
            }else{
                resultMap.put("isSuccess", false);
                resultMap.put("msg", "화자분리코드 데이터 오류:" + rxtx);
                return resultMap;
            }
            resultMap.put("spkrSprtnCd" , spkrSprtnCd);  // 화자분리코드

            String telStrtDt = fileNm.substring(0, 8);
            String telStrtTm = fileNm.substring(8, 14);

            //--------------------
            // STT ID
            //--------------------
            String sttId;
            String callid = (String) jsonObject.get("CALLID");
            String[] sttIdTmpArr = callid.split("_");

            if (sttIdTmpArr.length > 0) {
                sttId = sttIdTmpArr[0];
            } else {
                sttId = callid;
            }
            resultMap.put("sttId" , sttId);

            //--------------------
            // 내선번호
            //--------------------
            resultMap.put("intrnLineNum" , jsonObject.get("EXTENSION"));

            //--------------------
            // 고객전화번호
            //--------------------
            resultMap.put("custTelNum" , jsonObject.get("ANI"));

            //--------------------
            // 전화시작일자
            //--------------------
            resultMap.put("telStrtDt" , telStrtDt );

            //--------------------
            // 전화시작시간
            //--------------------
            resultMap.put("telStrtTm" , telStrtTm );

            //--------------------
            //STT
            //--------------------
            String sttData = (String) jsonObject.get("STT");
            if(sttData == null || sttData.isEmpty()){
                resultMap.put("isSuccess", false);
                resultMap.put("msg", "STT 데이터 없음");
                return resultMap;
            }
            Map<String, Object> sttMap;
            List<Map<String, Object>> sttList = new ArrayList<>();
            String[] sttDataArr = sttData.split("\\|");
            String artclStrtTm = null; // 발화시작시간
            String artclEndTm = null; // 발종료시간
            String sttCntnt = null;

            // 날짜지정
            String sttDtTm = telStrtDt + telStrtTm;
            SimpleDateFormat baseFmt = new SimpleDateFormat("yyyyMMddHHmmss");
            SimpleDateFormat timeFmt = new SimpleDateFormat("HHmmss");
            Date inputDate = baseFmt.parse(sttDtTm);
            Calendar calendar = Calendar.getInstance();

            for (String data : sttDataArr) {
                sttMap = new HashMap<>();
                Pattern pattern = Pattern.compile("<(\\d+)>(.*?)<(\\d+)>");
                Matcher matcher = pattern.matcher(data);

                // 정규 표현식에 일치하는 숫자와 텍스트를 찾아서 출력
                while (matcher.find()) {
                    artclStrtTm = matcher.group(1);
                    sttCntnt = matcher.group(2);
                    artclEndTm = matcher.group(3);
                }

                // 시작시간 지정
                calendar.setTime(inputDate);
                calendar.add(Calendar.MILLISECOND, Integer.parseInt(Objects.requireNonNull(artclStrtTm)));
                Timestamp artclStrtTimestamp = new Timestamp(calendar.getTimeInMillis());

                // 종료시간 지정
                calendar.setTime(inputDate);
                calendar.add(Calendar.MILLISECOND, Integer.parseInt(Objects.requireNonNull(artclEndTm)));
                Timestamp artclEndTimestamp = new Timestamp(calendar.getTimeInMillis());

                sttMap.put("artclStrtTm", timeFmt.format(artclStrtTimestamp));
                sttMap.put("artclEndTm", timeFmt.format(artclEndTimestamp));
                sttMap.put("sttCntnt", sttCntnt);
                sttList.add(sttMap);
            }
            resultMap.put("sttList" , sttList);


            //--------------------
            // KEYWORDS
            //--------------------
            // 데이터구조 ? 경찰=이슈|71020:71380,경찰=이슈|77620:77980
            List<Map<String, Object>> keywordsArrList = new ArrayList<>();
            String keywordsData = (String) jsonObject.get("KEYWORDS");
            if(keywordsData != null && !keywordsData.isEmpty()){
                String[] keywordsArr =  keywordsData.split(",");
                Map<String, Object> keywordsArrMap = new HashMap<>();

                for (String data : keywordsArr) {
                    keywordsArrMap = new HashMap<>();
                    String[] keywordSubArr = data.split("\\|");

                    // 키워드 관련 데이터(경찰=이슈)
                    String[] wordArr =  keywordSubArr[0].split("=");
                    keywordsArrMap.put("sttKywrdNm"         , wordArr[0]);      // 키워드명
                    keywordsArrMap.put("sttKywrdCtgryNm"    , wordArr[1]);      // 카테고리명

                    // 시간 관련 데이터(71020:71380)
                    String[] timeArr = keywordSubArr[1].split(":");

                    // 시작시간 지정
                    calendar.setTime(inputDate);
                    calendar.add(Calendar.MILLISECOND, Integer.parseInt(timeArr[0]));
                    Timestamp artclStrtTimestamp = new Timestamp(calendar.getTimeInMillis());

                    // 종료시간 지정
                    calendar.setTime(inputDate);
                    calendar.add(Calendar.MILLISECOND, Integer.parseInt(timeArr[1]));
                    Timestamp artclEndTimestamp = new Timestamp(calendar.getTimeInMillis());

                    keywordsArrMap.put("artclStrtTm"    , timeFmt.format(artclStrtTimestamp));  // 발화시작시간
                    keywordsArrMap.put("artclEndTm"     , timeFmt.format(artclEndTimestamp));   // 발화종료시간
                    keywordsArrList.add(keywordsArrMap);
                }
            }
            resultMap.put("keywords" , keywordsArrList);

            log.info("------------------------------------------");
            log.info("------------------------------------------");
            log.info("리턴?" + resultMap);
            log.info("------------------------------------------");
            log.info("------------------------------------------");
        } catch ( ParseException e) {
            log.error("[ParseException]" + e);
            resultMap.put("isSuccess", false);
            resultMap.put("msg", "json형식이 아닙니다." + e.getMessage());
            return resultMap;

        } catch (java.text.ParseException e) {
            log.error("[java.text.ParseException]" + e);
            resultMap.put("isSuccess", false);
            resultMap.put("msg", "날짜 형식이 아닙니다." + e.getMessage());
            return resultMap;
        }catch (Exception e){
            log.error("[Exception]" + e);
            resultMap.put("isSuccess", false);
            resultMap.put("msg", "알수 없는 오류 발생" + e.getMessage());
            return resultMap;
        }

        resultMap.put("isSuccess", true);
        return resultMap;
    }


    public Map<String, Object> fileToList (String sttId, String fileName, String intrnLineNum, String custTelNum) {
        Map<String, Object> resultMap = readFile(fileName);
        return resultMap;
    }
}

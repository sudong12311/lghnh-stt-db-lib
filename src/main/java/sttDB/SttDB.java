package sttDB;



import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import sttDB.util.DBUtil;
import sttDB.util.FileUtil;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;


public class SttDB {

    private static final String BASE_TIMEOUT = "60000";
    private static final Logger log = (Logger) LoggerFactory.getLogger(SttDB.class);

//    static ch.qos.logback.classic.Logger log =
//            (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(SttDB.class);

    /* DB info */
    private static String DB_URL;
    private static String DB_USER;
    private static String DB_PASSWORD;
    private static final String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";

    /* DB connect */
    Connection con = null;

    public static String TARGET_SERVER="";

    public static void main(String[] args) {


        //TODO
        //TEST
//        DB_URL = "jdbc:oracle:thin:@165.244.90.33:1525:gldbdev";
//        DB_USER = "cisdev";
//        DB_PASSWORD = "cisdev1234!";
//
//        String sttId = "2023091814381921054813017196_0_RX";
//        //String rxFileName = "Z:/main/09.업무/01.LG생활건강/03.고도화/02.작업/2023091814381921054813017196_0_RX.txt";
//        String fileName = "Z:/main/09.업무/01.LG생활건강/03.고도화/02.작업/stt/202310060900575120169117142_0_RX.txt";
//        String intrnLineNum = "17009";
//        String custTelNum = "01083730802";
//
//
//        for(int i=0; i<1; i++ ){
//            SttDB sttDb =  new SttDB();
//            sttDb.setLog("./logs/aaa/", "");
//            boolean flag = sttDb.getConnection(DB_URL, DB_USER, DB_PASSWORD, "");
//            log.info("getConnection flag:" + flag);
//            flag= true;
//            if(flag) {
//                sttDb.textResultToDB(sttId, fileName, intrnLineNum, custTelNum);
//            }
//        }

    }

    //public ch.qos.logback.classic.Logger loginfo() {
//        return log;
//    }

    public void setLog(String dir, String lv){

//        try{
//            // 현재일자 가져오기
//            LocalDate currentDate = LocalDate.now();
//            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
//            String formattedDate = currentDate.format(formatter);
//
//            // 유효한 디렉토리 경로인지 확인
//            //String folderPathPattern = "^(?:[a-zA-Z]:)?[\\\\/](?:[^\\\\/]+[\\\\/])*[^\\\\/]*$";
//
//            String folderPathPattern = "^/([A-Za-z0-9_]+/?)+$";
//            Pattern pattern = Pattern.compile(folderPathPattern);
//            Matcher matcher = pattern.matcher(dir);
//            if(!matcher.matches()) {
//                //System.out.println("///////////////////////////////////////////////////////////////matcher.matches dir?" + dir);
//                dir = "./logs/";
//            }
//
//            // 로그 경로 property 추가
//            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
//            loggerContext.putProperty("LOG_DIR", String.format("%s/", dir));
//            String logDir = loggerContext.getProperty("LOG_DIR");
//
//            //최상위 ROOT logger에 설정
//            //ch.qos.logback.classic.Logger log = loggerContext.getLogger("ROOT");
//            log =  (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(SttDB.class);
//            LoggerContext context = log.getLoggerContext();
//
//            // 시간이 지나 날짜별로 만들기 위해 설정
//            TimeBasedRollingPolicy<ILoggingEvent> policy = new TimeBasedRollingPolicy<>();
//            policy.setFileNamePattern(OptionHelper.substVars(
//                    logDir + "history/stt_db_lib_%d{yyyy-MM-dd}.log", context));
//            policy.setMaxHistory(30); // 최대 30일까지 로그 파일
//            policy.setContext(context);
//
//            // 로그작성 패턴
//            PatternLayoutEncoder encoder = new PatternLayoutEncoder();
//            encoder.setContext(context);
//            encoder.setPattern("%d{yyyy.MM.dd HH:mm:ss.SSS} [%thread] %highlight(%-5level) [%class{36}] - %msg%n");
//
//            RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
//            appender.setContext(context);
//            appender.setName("echo_log");
//            appender.setFile(OptionHelper.substVars(logDir + "/stt_db_lib_" +formattedDate+ "_1.log", context));
//            appender.setAppend(true);
//
//            appender.setPrudent(false);
//            appender.setRollingPolicy(policy);
//            appender.setEncoder(encoder);
//            policy.setParent(appender);
//
//            policy.start();
//            encoder.start();
//            appender.start();
//
//            if (lv.equalsIgnoreCase("DEBUG")) {
//                log.setLevel(Level.DEBUG);
//            }else if (lv.equalsIgnoreCase("INFO")) {
//                log.setLevel(Level.INFO);
//            }else if (lv.equalsIgnoreCase("ERROR")) {
//                log.setLevel(Level.ERROR);
//            }else{
//                log.setLevel(Level.DEBUG);
//            }
//
//            log.setAdditive(false); // true 이면 xml도 생성
//            log.addAppender(appender);
//            log.info(">>> LOG DIR :::  [{}]", logDir);
//        }catch (Exception e){
//            log.error("Exception" + e);
//        }

    }


    /**
     * DB connection
     * @param url jdbc URL
     * @param user DB연결계정
     * @param password DB비밀번호
     * @param timeout   DB연결 대기시간
     */
    public boolean getConnection(String url, String user, String password, String timeout){

        log.info("[SttDB] start =================>");
        // logback 환경설정 로그
        // DB등록할때 등록자명을 STT1, STT2으로 구분하기 위함
        Properties prop = new Properties();
        InputStream inputStream = SttDB.class.getClassLoader().getResourceAsStream("logback.properties");
        if(inputStream != null){
            try {
                prop.load(inputStream);
                TARGET_SERVER = prop.getProperty("server");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }


        log.info("[SttDB](getConnection) start url:{}, user:{}, password:{}, timeout{}" ,url ,user ,password ,timeout);

        DB_URL = url;
        DB_USER = user;
        DB_PASSWORD = password;
        String DB_TIMEOUT = (timeout == null || timeout.isEmpty()) ? BASE_TIMEOUT : timeout;
        try {

            if(con !=null && !con.isClosed()){
                return true;
            }

            Class.forName(DB_DRIVER);
            Properties properties = new Properties();
            properties.setProperty("user", DB_USER);
            properties.setProperty("password", password);
            properties.setProperty("oracle.jdbc.ReadTimeout", DB_TIMEOUT);
            properties.setProperty("oracle.net.CONNECT_TIMEOUT", DB_TIMEOUT);
            con = DriverManager.getConnection(url, properties);

            log.info("timeout:" + DB_TIMEOUT);
            log.debug("oracle isClosed?" + con.isClosed());

            // DB 연결 테스트
//            String sql = "SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') TO_DATE FROM DUAL";
//            Statement stmt = con.createStatement();
//            rs = stmt.executeQuery(sql);;
//            while (rs.next()) {
//                log.debug("oracle db test data:" + rs.getString("TO_DATE"));
//            }

        } catch (ClassNotFoundException e) {
            log.error("[SttDB](getConnection) ClassNotFoundException" + e);
            try {
                if ( con != null) con.close();
            } catch (SQLException ec) {
                log.error("[SttDB](closeConnection) SQLException" + ec);
            }
            return false;
        } catch (SQLException e) {
            log.error("[SttDB](getConnection) SQLException" + e);
            try {
                if ( con != null) con.close();
            } catch (SQLException ec) {
                log.error("[SttDB](closeConnection) SQLException" + ec);
            }
            return false;
        }catch (Exception e){
            try {
                if ( con != null) con.close();
            } catch (SQLException ec) {
                log.error("[SttDB](closeConnection) SQLException" + ec);
            }
            return false;
        }
        return true;
    }


    public void closeConnection() {
        log.info("[SttDB](closeConnection) call");
        try {
            if ( con != null) con.close();
        } catch (SQLException e) {
            log.error("[SttDB](closeConnection) SQLException" + e);
        }
    }

    public boolean textResultToDB(String sttId, String fileName, String intrnLineNum, String custTelNum) {
        log.info("[SttDB](textResultToDB) start - sttId:{} , fileName:{}, intrnLineNum:{}, custTelNum:{}" , sttId ,  fileName , intrnLineNum , custTelNum);

        try {
            /*
             * RX, TX파일 데이터 추출
             */
            FileUtil fileUtil = new FileUtil();
            Map<String, Object> resultFile = fileUtil.fileToList(sttId, fileName, intrnLineNum, custTelNum);
            boolean isSuccess = (boolean) resultFile.get("isSuccess");
            log.debug("파일추출 결과:" + isSuccess);

            if(!isSuccess){
                log.error("Error occurred in fileUtil " + resultFile.get("msg") );
                return false;
            }

            //  DB 연결이 닫힌 경우 재연결을 시도 한다.
            log.debug("con.getClientInfo()?" + con.isClosed());
            if ( con.isClosed()) {
                log.error("DB connection has been lost! re connect");
                boolean isSucess =  this.getConnection(DB_URL,DB_USER, DB_PASSWORD, null);
                if(!isSucess){
                    log.error("DB connection fail!!");
                    return false;
                }
            }

            /*
             * 파일에서 추출된 결과를 DB에 적재
             */
            DBUtil dBUtil = new DBUtil();
            // 1. STT관리 insert
            boolean isSttMgmt = dBUtil.insertSttMgmt(resultFile, con);
            // 2. STT관리상세 insert
            boolean isSttMgmtDtl = dBUtil.insertSttMgmtDtl(resultFile, con);
            // 3 STT키워드 insert
            boolean isSttKywrd = dBUtil.insertSttKywrd(resultFile, con);
            log.debug("insertSttMgmt result:{}", isSttMgmt);
            log.debug("insertSttMgmtDtl result:{}", isSttMgmtDtl);
            log.debug("insertSttKywrd result:{}", isSttKywrd);

        } catch (SQLException e) {
            log.error("[SttDB](textResultToDB) SQLException" + e);
            return false;
        }finally {

            try {
                log.info("프로세스 종료!!!!!!!!!!!!!!!!!con" + con);
                if ( con != null) con.close();
            } catch (SQLException e) {
                log.error("[SttDB](textResultToDB) Connection cannot be closed" + e);
            }
        }
        return true;
    }

}
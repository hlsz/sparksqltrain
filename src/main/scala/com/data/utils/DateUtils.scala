package com.data.utils

import java.text.{DecimalFormat, SimpleDateFormat}
import java.time.LocalDate
import java.util.{Calendar, Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer


object Test_Time001 {
  def main(args: Array[String]): Unit = {
    val nowdate = LocalDate.now()
    println("LocalDate.now()-->现在的时间是" + LocalDate.now())
    println("nowdate.plusDays-->明天是-->" + nowdate.plusDays(1))
    println("nowdate.minusDays-->昨天是-->" + nowdate.minusDays(1))
    println("nowdate.plusMonths-->今天加一个月-->" + nowdate.plusMonths(1))
    println("nowdate.minusMonths-->今天减一个月-->" + nowdate.minusMonths(1))
    println("nowdate.getDayOfYear-->今天是今年的第" + nowdate.getDayOfYear + "天")
    println("nowdate.getDayOfMonth->这个月有" + nowdate.getDayOfMonth + "天")
    println("nowdate.getDayOfWeek-->今天星期" + nowdate.getDayOfWeek)
    println("nowdate.getMonth-->这个月是" + nowdate.getMonth)
  }
}

object DateUtils {
  //FastDateFormat
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val DATE_FORMAT = "yyyy-MM-dd"

  val DATE_INT_FORMAT = "yyyyMMdd"

  val DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

  val DATE_FORMAT_CHINESE = "yyyy年M月d日"

  def intervalMonths(fromDate:String, toDate:String): Int ={
    val sdf = new SimpleDateFormat(DATE_FORMAT)
    val str1 = fromDate
    val str2 = toDate
    val bef = Calendar.getInstance()
    val aft = Calendar.getInstance()
    bef.setTime(sdf.parse(str1))
    aft.setTime(sdf.parse(str2))
    var surplus = aft.get(Calendar.DATE) - bef.get(Calendar.DATE)
    val result = aft.get(Calendar.MONTH) - bef.get(Calendar.MONTH)
    val month = (aft.get(Calendar.YEAR) - bef.get(Calendar.YEAR)) * 12
//    println(surplus)
    surplus = if (surplus < 0) {1} else {0}
//    println(surplus)
//    println("相差月份：" + (Math.abs(month + result) + surplus))
    Math.abs(month + result) + surplus
  }

  def getDaysOfMonth( year:Int,  month:Int) :Int = {
    val calendar = Calendar.getInstance()
    calendar.set(year, month - 1, 1)
    calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
  }

  def getYear( date:Date):Int = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.get(Calendar.YEAR)
  }

  def getMonth( date:Date) :Int = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.get(Calendar.MONTH) + 1
  }
  def  getDay( date:Date):Int = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.get(Calendar.DATE)
  }


  def calDiffMonth(startDate:String, endDate:String) :Int ={
    var result=0
    try {
      val sfd=new SimpleDateFormat("yyyy-MM-dd")
      val start = sfd.parse(startDate)
      val end = sfd.parse(endDate)
      val startYear=getYear(start)
      val startMonth=getMonth(start)
      val startDay=getDay(start)
      val endYear=getYear(end)
      val endMonth=getMonth(end)
      var endDay=getDay(end)
      if (startDay > endDay){ //1月17  大于 2月28
        if (endDay==getDaysOfMonth(getYear(new Date()),2)){   //也满足一月
          result=(endYear-startYear)*12+endMonth-startMonth
        }else{
          result=(endYear-startYear)*12+endMonth-startMonth-1
        }
      }else{
        result=(endYear-startYear)*12+endMonth-startMonth
      }
    } catch  {
      case e:Exception => -1
    }
    result
  }

  def getMonthDiff(d1:Date, d2:Date) {
        val c1 = Calendar.getInstance()
        val c2 = Calendar.getInstance()
        c1.setTime(d1)
        c2.setTime(d2)
        val year1 = c1.get(Calendar.YEAR)
        val year2 = c2.get(Calendar.YEAR)
        val month1 = c1.get(Calendar.MONTH)
        val month2 = c2.get(Calendar.MONTH)
        val day1 = c1.get(Calendar.DAY_OF_MONTH)
        val day2 = c2.get(Calendar.DAY_OF_MONTH)
        // 获取年的差值
        var yearInterval = year1 - year2
        // 如果 d1的 月-日 小于 d2的 月-日 那么 yearInterval-- 这样就得到了相差的年数
        if (month1 < month2 || month1 == month2 && day1 < day2)
        {
          yearInterval = yearInterval-1
        }
        // 获取月数差值
        var monthInterval = (month1 + 12) - month2
        if (day1 < day2){
          monthInterval = monthInterval - 1
        }
        monthInterval %= 12
        val monthsDiff = Math.abs(yearInterval * 12 + monthInterval);
//        return monthsDiff
  }


  def intevalSeconds(fromDate:String, toDate:String): Long = {
    val df = new SimpleDateFormat(DateUtils.DATE_TIME_FORMAT)
    try {
      //前的时间
      val fd = df.parse(fromDate)
      //后的时间
      val td: Date = df.parse(toDate)
      //两时间差,精确到毫秒
      val diff: Long = td.getTime() - fd.getTime()
      val seconds: Long = diff % 86400000 % 3600000 % 60000 / 1000
      seconds
    } catch {
      case e:Exception => 0L
    }
  }

  def intervalDays(fromDate:String, toDate:String): Long = {
    val df = new SimpleDateFormat(DateUtils.DATE_FORMAT)
    try {
      //前的时间
      val fd = df.parse(fromDate)
      //后的时间
      val td: Date = df.parse(toDate)
      //两时间差,精确到毫秒
      val diff: Long = td.getTime() - fd.getTime()
      val day:Long  = Math.abs(diff / 86400000)
      day
    } catch {
      case e:Exception => 0L
    }
  }

  def interDays(fromDate:String,toDate:String): Long ={

    val sdf = new SimpleDateFormat(DateUtils.DATE_FORMAT )
    //跨年的情况会出现问题哦
    //如果时间为：2016-03-18 11:59:59 和 2016-03-19 00:00:01的话差值为 1
    val fDate = sdf.parse(fromDate)
    val oDate = sdf.parse(toDate)
    val aCalendar: Calendar = Calendar.getInstance
    aCalendar.setTime(fDate)
    val day1: Int = aCalendar.get(Calendar.DAY_OF_YEAR)
    aCalendar.setTime(oDate)
    val day2: Int = aCalendar.get(Calendar.DAY_OF_YEAR)
    val days = Math.abs(day2 - day1)
    days.toLong
  }

  def diffDateInterval(fromDate:String, toDate:String): Unit =
  {
//    String fromDate = "2013-04-16 08:29:12";
//    String toDate = "2013-04-20 09:44:29";
    val df = new SimpleDateFormat(DateUtils.DATE_TIME_FORMAT)
    try {
      //前的时间
      val fd = df.parse(fromDate)
      //后的时间
      val td:Date = df.parse(toDate)
      //两时间差,精确到毫秒
      val diff:Long = td.getTime() - fd.getTime()
      val day:Long  = diff / 86400000                        //以天数为单位取整
      val hour:Long = diff % 86400000 / 3600000               //以小时为单位取整
      val min:Long  = diff % 86400000 % 3600000 / 60000       //以分钟为单位取整
      val seconds:Long = diff % 86400000 % 3600000 % 60000 / 1000   //以秒为单位取整
      //天时分秒
//      println("两时间差---> " +day+"天"+hour+"小时"+min+"分"+seconds+"秒");
    } catch {
      case e:Exception => e.printStackTrace()
    }

  }


  def isValidDate(str:String) : Boolean  = {
    val  strrepl = str
    if (str.indexOf("/") > 0) {
     val  strrepl = str.replaceAll("""/""", """-""")
    }
    var convertSuccess = true
    val format = new SimpleDateFormat("yyyy-MM-dd")
    try {
      format.setLenient(false)
      format.parse(strrepl)
    } catch  {
      case e:Exception  => false
    }
    return convertSuccess
  }


  /**
    * 获取两个日期之间的日期
    * @param start 开始日期
    * @param end 结束日期
    * @return 日期集合
    */
  def getBetweenDates(start: String, end: String) = {
    val startData = new SimpleDateFormat("yyyyMMdd").parse(start); //定义起始日期
    val endData = new SimpleDateFormat("yyyyMMdd").parse(end); //定义结束日期

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var buffer = new ListBuffer[String]
    buffer += dateFormat.format(startData.getTime())
    val tempStart = Calendar.getInstance()

    tempStart.setTime(startData)
    tempStart.add(Calendar.DAY_OF_YEAR, 1)

    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endData)
    while (tempStart.before(tempEnd)) {
      // result.add(dateFormat.format(tempStart.getTime()))
      buffer += dateFormat.format(tempStart.getTime())
      tempStart.add(Calendar.DAY_OF_YEAR, 1)
    }
    buffer += dateFormat.format(endData.getTime())

    buffer.toList
  }

  //时间字符串=>时间戳
  def convertDateStr2TimeStamp(dateStr: String, pattern: String): Long = {
    new SimpleDateFormat(pattern).parse(dateStr).getTime
  }

  //时间字符串+天数=>时间戳
  def dateStrAddDays2TimeStamp(dateStr: String, pattern: String, days: Int): Long = {
    convertDateStr2Date(dateStr, pattern).plusDays(days).toDate().getTime
  }

  //时间字符串=>日期
  def convertDateStr2Date(dateStr: String, pattern: String): DateTime = {
    new DateTime(new SimpleDateFormat(pattern).parse(dateStr))
  }

  //时间戳=>日期
  def convertTimeStamp2Date(timestamp: Long): DateTime = {
    new DateTime(timestamp)
  }

  //时间戳=>字符串
  def convertTimeStamp2DateStr(timestamp: Long, pattern: String): String = {
    new DateTime(timestamp).toString(pattern)
  }

  //时间戳=>小时数
  def convertTimeStamp2Hour(timestamp: Long): Long = {
    new DateTime(timestamp).hourOfDay().getAsString().toLong
  }

  //时间戳=>分钟数
  def convertTimeStamp2Minute(timestamp: Long): Long = {
    new DateTime(timestamp).minuteOfHour().getAsString().toLong
  }

  //时间戳=>秒数
  def convertTimeStamp2Sec(timestamp: Long): Long = {
    new DateTime(timestamp).secondOfMinute().getAsString.toLong
  }



  def addZero(hourOrMin: String): String = {
    if (hourOrMin.toInt <= 9)
      "0" + hourOrMin
    else
      hourOrMin

  }

  def delZero(hourOrMin: String): String = {
    var res = hourOrMin
    if (!hourOrMin.equals("0") && hourOrMin.startsWith("0"))
      res = res.replaceAll("^0","")
    res
  }

  def dateStrPatternOne2Two(time: String): String = {
   convertTimeStamp2DateStr(convertDateStr2TimeStamp(time, DATE_FORMAT), DATE_INT_FORMAT)
  }

  //获取星期几
  def dayOfWeek(dateStr: String): Int = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(dateStr)
    //    val sdf2 = new SimpleDateFormat("EEEE")
    //    sdf2.format(date)
    val cal = Calendar.getInstance();
    cal.setTime(date);
    var w = cal.get(Calendar.DAY_OF_WEEK) - 1;

    //星期天 默认为0
    if (w <= 0)
      w = 7
    w
  }

  //判断是否是周末
  def isRestday(date: String): Boolean = {
    val dayNumOfWeek = dayOfWeek(date)
    dayNumOfWeek == 6 || dayNumOfWeek == 7
  }


  //    获取今天日期
  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
    var today = dateFormat.format( now )
    today
  }

  //    获取昨天的时间
  def getYesterday():String= {
    var dateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
    var cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  //    获取本周开始日期
  def getNowWeekStart():String={
    var day:String=""
    var cal:Calendar =Calendar.getInstance();
    var df =  FastDateFormat.getInstance("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    day=df.format(cal.getTime())
    day
  }

  //    获取本周末日期
  def getNowWeekEnd():String={
    var day:String=""
    var cal:Calendar =Calendar.getInstance();
    var df = FastDateFormat.getInstance("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);//这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1)// 增加一个星期，才是我们中国人的本周日的日期
    day=df.format(cal.getTime())
    day
  }

  //    本月的第一天
  def getNowMonthStart():String= {
    var day: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df = FastDateFormat.getInstance("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    day = df.format(cal.getTime()) //本月第一天
    day
  }

  //    本月最后一天
  def getNowMonthEnd():String={
    var day:String=""
    var cal:Calendar =Calendar.getInstance();
    var df = FastDateFormat.getInstance("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE,-1)
    day=df.format(cal.getTime())//本月最后一天
    day
  }

  //    将时间戳转化成日期/时间
  def DateFormat(time:String):String={
    var sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    var date:String = sdf.format(new Date((time.toLong)))
    date
  }

  //    计算时间差
  def getCoreTime(start_time:String,end_Time:String)={
    var df = FastDateFormat.getInstance("HH:mm:ss")
    val begin = start_time
    val end = end_Time
    var between = (end.toLong - begin.toLong)/1000  //转化成秒
    var minute = between % 60 //转化为分钟
    val hour = between % (60*60) //转化为小时
  }

  def dateToInt(date: Date): Int ={
    val sdf = new SimpleDateFormat(DateUtils.DATE_INT_FORMAT)
    val dateStr = sdf.format(date)
    Integer.valueOf(dateStr)
  }

  def intToDate(date: Int): Date ={
    val dateFormat = new SimpleDateFormat(DateUtils.DATE_INT_FORMAT)
    val str = String.valueOf(date)
    dateFormat.parse(str)
  }
  def intToDateStr(date: Int): String ={
    val value = intToDate(date)
    val sdf =  new SimpleDateFormat(DateUtils.DATE_FORMAT)
    sdf.format(value.getTime)
  }
  def intToDateStr(date: Int, format:String): String ={
    val value = intToDate(date)
    val sdf =  new SimpleDateFormat(format)
    sdf.format(value.getTime)
  }

  def longToDate(dateLong : Long): Date ={
    val timeDate: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateLong * 1000L)
    val df = new SimpleDateFormat(DateUtils.DATE_TIME_FORMAT)
    df.parse(timeDate)
  }
  def dateToLong(date: String): Long ={
    val timeDate: Date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date)
    val timeLong: Long = timeDate.getTime
    timeLong
  }

  def dateFormat(date : Date, format : String): String = {
    val df = new SimpleDateFormat(format)
    val datestr = df.format(date)
    datestr
  }
  def dateFormat(date : Int, format : String): String = {
    val df = new SimpleDateFormat(format)
    val datestr = df.format(date)
    datestr
  }

  def addOrMinusDay(date : Date, day : Long): Date = {
    val time = date.getTime(); // 得到指定日期的毫秒数
    val days = day*24*60*60*1000; // 要加上的天数转换成毫秒数
    val addTime = time +  days; // 相加得到新的毫秒数
    new Date(addTime); // 将毫秒数转换成日期
  }

  def addOrMinusDayToLong(date : Date, day : Long): Long = {
    val time = date.getTime(); // 得到指定日期的毫秒数
    val days = day*24*60*60*1000; // 要加上的天数转换成毫秒数
    val addTime = time +  days; // 相加得到新的毫秒数
    val sdf = new SimpleDateFormat(DateUtils.DATE_FORMAT)
    sdf.format(new Date(addTime)).toLong; // 将毫秒数转换成日期
  }

  /**
    * 获取当前日期
    *
    * @return
    *
    */
  def getCurrentDateStr: String = {
    val df = new SimpleDateFormat(DateUtils.DATE_FORMAT)
    val datestr = df.format(new Date())
    datestr
  }

  def getCurrentDateStr(format:String): String = {
    val df = new SimpleDateFormat(format)
    val datestr = df.format(new Date())
    datestr
  }

  def getCurrentDate: Date = {
    val currDate =  new Date()
    currDate
  }

  /**
    * 获取当前日期时间
    *
    * @return
    *
    */
  def getCurrentDateTime: String = {
    val df = new SimpleDateFormat(DateUtils.DATE_TIME_FORMAT)
    var datestr = df.format(new Date)
    datestr
  }

  def getCurrentDateTime(Dateformat: String): String = {
    val df = new SimpleDateFormat(Dateformat)
    val datestr = df.format(new Date)
    datestr
  }

  def dateToDateTime(date: Date): String = {
    val df = new SimpleDateFormat(DateUtils.DATE_TIME_FORMAT)
    val datestr = df.format(date)
    datestr
  }

  /**
    * 将字符串日期转换为日期格式
    *
    * @param datestr
    * @return
    *
    */
  def stringToDate(datestr: String): Date = {
    if (datestr == null || datestr == "") return null
    var date = new Date
    val df = new SimpleDateFormat(DateUtils.DATE_FORMAT)
    try
      date = df.parse(datestr)
    catch {
      case e: Exception  =>
        date = DateUtils.stringToDate(datestr, "yyyyMMdd")
    }
    date
  }

  /**
    * 将字符串日期转换为日期格式
    * 自定義格式
    *
    * @param datestr
    * @return
    *
    */
  def stringToDate(datestr: String, dateformat: String): Date = {
    var date = new Date()
    val df = new SimpleDateFormat(dateformat)
    try
      date = df.parse(datestr)
    catch {
      case e: Exception =>
        e.printStackTrace
    }
    date
  }

  /**
    * 将日期格式日期转换为字符串格式
    *
    * @param date
    * @return
    *
    */
  def dateToString(date: Date): String = {
    val df = new SimpleDateFormat(DateUtils.DATE_FORMAT)
    val datestr = df.format(date)
    datestr
  }

  /**
    * 将日期格式日期转换为字符串格式 自定義格式
    *
    * @param date
    * @param dateformat
    * @return
    */
  def dateToString(date: Date, dateformat: String): String = {
    var datestr = ""
    val df = new SimpleDateFormat(dateformat)
    try{
      datestr = df.format(date)
      datestr
    }catch{
      case e:Exception => {
          e.printStackTrace()
      }
    }
    datestr

  }

  /**
    * 获取日期的DAY值
    *
    * @param date
    * 输入日期
    * @return
    *
    */
  def getDayOfDate(date: Date): Int = {
    var d = 0
    val cd = Calendar.getInstance
    cd.setTime(date)
    d = cd.get(Calendar.DAY_OF_MONTH)
    d
  }

  /**
    * 获取日期的MONTH值
    *
    * @param date
    * 输入日期
    * @return
    *
    */
  def getMonthOfDate(date: Date): Int = {
    var m = 0
    val cd = Calendar.getInstance
    cd.setTime(date)
    m = cd.get(Calendar.MONTH) + 1
    m
  }

  /**
    * 获取日期的YEAR值
    *
    * @param date
    * 输入日期
    * @return
    *
    */
  def getYearOfDate(date: Date): Int = {
    var y = 0
    val cd = Calendar.getInstance
    cd.setTime(date)
    y = cd.get(Calendar.YEAR)
    y
  }

  /**
    * 获取星期几
    *
    * @param date
    * 输入日期
    * @return
    *
    */
  def getWeekOfDate(date: Date): Int = {
    var wd = 0
    val cd = Calendar.getInstance
    cd.setTime(date)
    wd = cd.get(Calendar.DAY_OF_WEEK) - 1
    wd
  }

  /**
    * 获取输入日期的当月第一天
    *
    * @param date
    * 输入日期
    * @return
    *
    */
  def getFirstDayOfMonth(date: Date): Date = {
    val cd = Calendar.getInstance
    cd.setTime(date)
    cd.set(Calendar.DAY_OF_MONTH, 1)
    cd.getTime
  }

  /**
    * 获得输入日期的当月最后一天
    *
    * @param date
    */
  def getLastDayOfMonth(date: Date): Date = DateUtils.addDay(DateUtils.getFirstDayOfMonth(DateUtils.addMonth(date, 1)), -1)

  /**
    * 判断是否是闰年
    *
    * @param date
    * 输入日期
    * @return 是true 否false
    *
    */
  def isLeapYEAR(date: Date): Boolean = {
    val cd = Calendar.getInstance
    cd.setTime(date)
    val year = cd.get(Calendar.YEAR)
    if (year % 4 == 0 && year % 100 != 0 | year % 400 == 0) true
    else false
  }

  /**
    * 根据整型数表示的年月日，生成日期类型格式
    *
    * @param year
    * 年
    * @param month
    * 月
    * @param day
    * 日
    * @return
    *
    */
  def getDateByYMD(year: Int, month: Int, day: Int): Date = {
    val cd = Calendar.getInstance
    cd.set(year, month - 1, day)
    cd.getTime
  }

  /**
    * 获取年周期对应日
    *
    * @param date
    * 输入日期
    * @param iyear
    * 年数  負數表示之前
    * @return
    *
    */
  def getYearCycleOfDate(date: Date, iyear: Int): Date = {
    val cd = Calendar.getInstance
    cd.setTime(date)
    cd.add(Calendar.YEAR, iyear)
    cd.getTime
  }

  /**
    * 获取月周期对应日
    *
    * @param date
    * 输入日期
    * @param i
    * @return
    *
    */
  def getMonthCycleOfDate(date: Date, i: Int): Date = {
    val cd = Calendar.getInstance
    cd.setTime(date)
    cd.add(Calendar.MONTH, i)
    cd.getTime
  }

  /**
    * 计算 fromDate 到 toDate 相差多少年
    *
    * @param fromDate
    * @param toDate
    * @return 年数
    *
    */
  def getYearByMinusDate(fromDate: Date, toDate: Date): Int = {
    val df = Calendar.getInstance
    df.setTime(fromDate)
    val dt = Calendar.getInstance
    dt.setTime(toDate)
    dt.get(Calendar.YEAR) - df.get(Calendar.YEAR)
  }

  /**
    * 计算 fromDate 到 toDate 相差多少个月
    *
    * @param fromDate
    * @param toDate
    * @return 月数
    *
    */
  def getMonthByMinusDate(fromDate: Date, toDate: Date): Int = {
    val df = Calendar.getInstance
    df.setTime(fromDate)
    val dt = Calendar.getInstance
    dt.setTime(toDate)
    dt.get(Calendar.YEAR) * 12 + dt.get(Calendar.MONTH) - (df.get(Calendar.YEAR) * 12 + df.get(Calendar.MONTH))
  }

  /**
    * 计算 fromDate 到 toDate 相差多少天
    *
    * @param fromDate
    * @param toDate
    * @return 天数
    *
    */
  def getDayByMinusDate(fromDate: Object, toDate: Object): Long = {
    val f = DateUtils.chgObject(fromDate)
    val t = DateUtils.chgObject(toDate)
    val fd = f.getTime
    val td = t.getTime
    (td - fd) / (24L * 60L * 60L * 1000L)
  }

  /**
    * 计算年龄
    *
    * @param birthday
    * 生日日期
    * @param calcDate
    * 要计算的日期点
    * @return
    *
    */
  def calcAge(birthday: Date, calcDate: Date): Int = {
    val cYear = DateUtils.getYearOfDate(calcDate)
    val cMonth = DateUtils.getMonthOfDate(calcDate)
    val cDay = DateUtils.getDayOfDate(calcDate)
    val bYear = DateUtils.getYearOfDate(birthday)
    val bMonth = DateUtils.getMonthOfDate(birthday)
    val bDay = DateUtils.getDayOfDate(birthday)
    if (cMonth > bMonth || (cMonth == bMonth && cDay > bDay)) cYear - bYear
    else cYear - 1 - bYear
  }

  /**
    * 从身份证中获取出生日期
    *
    * @param idno
    * 身份证号码
    * @return
    *
    */
  def getBirthDayFromIDCard(idno: String): String = {
    val cd = Calendar.getInstance
    if (idno.length == 15) {
      cd.set(Calendar.YEAR, Integer.valueOf("19" + idno.substring(6, 8)).intValue)
      cd.set(Calendar.MONTH, Integer.valueOf(idno.substring(8, 10)).intValue - 1)
      cd.set(Calendar.DAY_OF_MONTH, Integer.valueOf(idno.substring(10, 12)).intValue)
    }
    else if (idno.length == 18) {
      cd.set(Calendar.YEAR, Integer.valueOf(idno.substring(6, 10)).intValue)
      cd.set(Calendar.MONTH, Integer.valueOf(idno.substring(10, 12)).intValue - 1)
      cd.set(Calendar.DAY_OF_MONTH, Integer.valueOf(idno.substring(12, 14)).intValue)
    }
    DateUtils.dateToString(cd.getTime)
  }

  /**
    * 在输入日期上增加（+）或减去（-）天数
    *
    * @param date
    * 输入日期
    * @param
    * 要增加或减少的天数
    */
  def addDay(date: Date, iday: Int): Date = {
    val cd = Calendar.getInstance
    cd.setTime(date)
    cd.add(Calendar.DAY_OF_MONTH, iday)
    cd.getTime
  }
  /**
    * 在输入日期上增加（+）或减去（-）月份
    *
    * @param date
    * 输入日期
    * @param imonth
    * 要增加或减少的月分数
    */
  def addMonth(date: Date, imonth: Int): Date = {
    val cd = Calendar.getInstance
    cd.setTime(date)
    cd.add(Calendar.MONTH, imonth)
    cd.getTime
  }

  /**
    * 在输入日期上增加（+）或减去（-）年份
    *
    * @param date
    * 输入日期
    * @param iyear
    * 要增加或减少的年数
    */
  def addYear(date: Date, iyear: Int): Date = {
    val cd = Calendar.getInstance
    cd.setTime(date)
    cd.add(Calendar.YEAR, iyear)
    cd.getTime
  }

  /**
    * 將OBJECT類型轉換為Date
    *
    * @param date
    * @return
    */
  def chgObject(date: Object): Date = {
    if (date != null && date.isInstanceOf[Date]) return date.asInstanceOf[Date]
    if (date != null && date.isInstanceOf[String]) return DateUtils.stringToDate(date.asInstanceOf[String])
    null
  }

  def getAgeByBirthday(date: String): Long = {
    val birthday = stringToDate(date, "yyyy-MM-dd")
    val sec = new Date().getTime - birthday.getTime
    val age = sec / (1000 * 60 * 60 * 24) / 365
    age
  }

  def parse(time:String){
//    TARGET_FORMAT.format(new Date(getTime(time)))
    return "1";
  }

  /**
    * 获取输入日志Long类型的时间
    *
    */
//  def getTime(time: String): Long ={
//    try{
////      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime()
//    }catch {
//      case e:Exception =>{
//          0L
//      }
//    }
//  }

  //核心工作时间，迟到早退等的的处理
  def getCoreTime2(start_time:String,end_Time:String)={
    var df:SimpleDateFormat=new SimpleDateFormat("HH:mm:ss")
    var begin:Date=df.parse(start_time)
    var end:Date = df.parse(end_Time)
    var between:Long=(end.getTime()-begin.getTime())/1000//转化成秒
    var hour:Float=between.toFloat/3600
    var decf:DecimalFormat=new DecimalFormat("#.00")
    decf.format(hour)//格式化

  }


  /**
    * 获取当前时间的上一个月的第一天与最后一天
    * @return
    */
  def getLastMonth():(String,String,String,String)={

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cals: Calendar = Calendar.getInstance()
    cals.add(Calendar.MONTH,-1)
    var monthstrs: String = ""
    var yearstrs: String = ""
    yearstrs = cals.get(Calendar.YEAR) + ""
    if (cals.get(Calendar.MONTH) + 1 < 10) {
      monthstrs = "0" + (cals.get(Calendar.MONTH) + 1) + ""
    } else {
      monthstrs = (cals.get(Calendar.MONTH) + 1) + ""
    }

    cals.set(Calendar.DAY_OF_MONTH,1)
    val firstDay=dateFormat.format(cals.getTime())
    //获取前月的最后一天
    val cale:Calendar = Calendar.getInstance()
    cale.set(Calendar.DAY_OF_MONTH,0)
    val lastDay=dateFormat.format(cale.getTime())

    (yearstrs,monthstrs,firstDay,lastDay)
  }

  /**
    *  获取当前时间的上一个自然周集合：
    * @return
    */
  def getLastWeek():List[(String,String,String)]={
    var list=List(("","",""))
    list = list.init
    var num = 7
    val date= new Date()
    var week = new Date().getDay()
    if(week == 0){//周天的时候
      num += 7
      week = 7
    }else{
      num = num + week - 1
    }
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    for(i <- week to num) {
      val newDate2 = new Date(date.getTime() - i * 24 * 60 * 60 * 1000)
      val cals: Calendar = Calendar.getInstance()
      cals.setTime(newDate2)
      var monthstrs: String = ""
      var yearstrs: String = ""
      yearstrs = cals.get(Calendar.YEAR) + ""
      if (cals.get(Calendar.MONTH) + 1 < 10) {
        monthstrs = "0" + (cals.get(Calendar.MONTH) + 1) + ""
      } else {
        monthstrs = (cals.get(Calendar.MONTH) + 1) + ""
      }
      list.::=(yearstrs, monthstrs, dateFormat.format(cals.getTime))

    }
    list
  }

  def main(args:Array[String]): Unit ={
//    print(parse("[10/Nov/2016:00:01:02 +0800]"))

    val rs = intervalMonths("2017-01-01","2017-09-01")
    println(rs)
//    val months = getMonthDiff(new Date("2018-01-01"), new Date("2017-01-01"))
//    println(months)
    val month2 = calDiffMonth("2017-01-01","2017-09-01")
    println(month2)

    val str =intToDate(20180101)
    println("str:"+str)

    val day =intervalDays("2018-01-01","2018-03-01")
    println(day)

    val days = interDays("2018-03-01","2018-01-01")
    println(days)
  }

}

package com.data.utils

import java.text.DecimalFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  import java.text.SimpleDateFormat
  import java.util.Calendar

  val DATE_FORMAT = "yyyy-MM-dd"

  val DATE_INT_FORMAT = "yyyyMMdd"

  val DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

  val DATE_FORMAT_CHINESE = "yyyy年M月d日"

  def dateToLong(date: String): Long ={
    val timeDate: Date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date)
    val timeLong: Long = timeDate.getTime
    timeLong
  }

  def dateToInt(date: Date): Int ={
    val sdf = new SimpleDateFormat(DateUtils.DATE_INT_FORMAT)
    val dateStr = sdf.format(date)
    Integer.valueOf(dateStr)
  }

  def intToDate(date: Int): Date ={
    val dateFormat = new SimpleDateFormat(DateUtils.DATE_FORMAT)
    val str = String.valueOf(date)
    dateFormat.parse(str.substring(0,4)+"-"+str.substring(4,6)+"-"+str.substring(6,8))
  }

  def longToDate(dateLong : Long): Date ={
    val timeDate: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateLong * 1000L)
    val df = new SimpleDateFormat(DateUtils.DATE_TIME_FORMAT)
    df.parse(timeDate)
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
      case e:Exception =>{
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
    * @param imonth
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
    * @param imonth
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
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 获取输入日志Long类型的时间
    *
    */
  def getTime(time: String): Long ={
    try{
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime()
    }catch{
      case e:Exception =>{
          0L
      }
    }
  }

  //核心工作时间，迟到早退等的的处理
  def getCoreTime(start_time:String,end_Time:String)={
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
    print(parse("[10/Nov/2016:00:01:02 +0800]"))
  }

}

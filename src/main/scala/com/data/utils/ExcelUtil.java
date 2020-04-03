package com.data.utils;


import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class ExcelUtil {

    public static final String XLSX = ".xlsx";
    public static final String XLS=".xls";

    /**
     * 获取Excel文件xls、xlsx都支持）
     * @param file
     * @return 解析excle后的Json数据
     * @throws IOException
     * @throws FileNotFoundException
     * @throws InvalidFormatException
     */
    public static JSONArray readExcel(File file) throws FileNotFoundException, IOException, InvalidFormatException{
        int res = checkFile(file);
        if (res == 0) {
            throw new NullPointerException("the file is null.");
        }else if (res == 1) {
            return readXLSX(file);
        }else if (res == 2) {
            return readXLS(file);
        }
        throw new IllegalAccessError("the file["+file.getName()+"] is not excel file.");
    }

    /**
     * 判断File文件的类
     * @param file 传入的文
     * @return 0-文件为空-XLSX文件-XLS文件-其他文件
     */
    public static int checkFile(File file){
        if (file==null) {
            return 0;
        }
        String flieName = file.getName();
        if (flieName.endsWith(XLSX)) {
            return 1;
        }
        if (flieName.endsWith(XLS)) {
            return 2;
        }
        return 3;
    }

    /**
     * 读取XLSX文件
     * @param file
     * @return
     * @throws IOException
     * @throws InvalidFormatException
     */
    public static JSONArray readXLSX(File file) throws InvalidFormatException, IOException{
        Workbook book = new XSSFWorkbook(file);
        Sheet sheet = book.getSheetAt(0);
        return read(sheet, book);
    }

    /**
     * 读取XLS文件
     * @param file
     * @return
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static JSONArray readXLS(File file) throws FileNotFoundException, IOException{
        POIFSFileSystem poifsFileSystem = new POIFSFileSystem(new FileInputStream(file));
        Workbook book = new HSSFWorkbook(poifsFileSystem);
        Sheet sheet = book.getSheetAt(0);
        return read(sheet, book);
    }

    /**
     * 解析数据
     * @param sheet 表格sheet对象
     * @param book 用于流关
     * @return
     * @throws IOException
     */
    public static JSONArray read(Sheet sheet,Workbook book) throws IOException{
        int rowStart = sheet.getFirstRowNum();  // 首行下标
        int rowEnd = sheet.getLastRowNum(); // 尾行下标
// 如果首行与尾行相同，表明只有��，直接返回空数组
        if (rowStart == rowEnd) {
            book.close();
            return new JSONArray();
        }
// 获取第一行JSON对象
        Row firstRow = sheet.getRow(rowStart);
        int cellStart = firstRow.getFirstCellNum();
        int cellEnd = firstRow.getLastCellNum();
        Map<Integer, String> keyMap = new HashMap<Integer, String>();
        for (int j = cellStart; j < cellEnd; j++) {
            keyMap.put(j,getValue(firstRow.getCell(j), rowStart, j, book, true));
        }
// 获取每行JSON对象的
        JSONArray array = new JSONArray();
        for(int i = rowStart+1; i <= rowEnd ; i++) {
            Row eachRow = sheet.getRow(i);
            JSONObject obj = new JSONObject();
            StringBuffer sb = new StringBuffer();
            for (int k = cellStart; k < cellEnd; k++) {
                if (eachRow != null) {
                    String val = getValue(eachRow.getCell(k), i, k, book, false);
                    sb.append(val); // ��数据添加到里面，用于判断该行是否为空
                    obj.put(keyMap.get(k),val);
                }
            }
            if (sb.toString().length() > 0) {
                array.add(obj);
            }
        }
        book.close();
        return array;
    }

    /**
     * 获取每个单元格的数据
     * @param cell 单元格对
     * @param rowNum 第几
     * @param index 该行第几
     * @param book 主要用于关闭
     * @param isKey 是否为键：true-是，false-不是如果解析Json键，值为空时报错；如果不是Json键，值为空不报错
     * @return
     * @throws IOException
     */
    public static String getValue(Cell cell,int rowNum,int index,Workbook book,boolean isKey) throws IOException{

// 空白或空
        if (cell == null || cell.getCellType()==Cell.CELL_TYPE_BLANK ) {
            if (isKey) {
                book.close();
                throw new NullPointerException(String.format("the key on row %s index %s is null ", ++rowNum,++index));
            }else{
                return "";
            }
        }

// 0. 数字 类型
        if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
            if (HSSFDateUtil.isCellDateFormatted(cell)) {
                Date date = cell.getDateCellValue();
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return df.format(date);
            }
            String val = cell.getNumericCellValue()+"";
            val = val.toUpperCase();
            if (val.contains("E")) {
                val = val.split("E")[0].replace(".", "");
            }
            return val;
        }

// 1. String类型
        if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
            String val = cell.getStringCellValue();
            if (val == null || val.trim().length()==0) {
                if (book != null) {
                    book.close();
                }
                return "";
            }
            return val.trim();
        }

// 2. 公式 CELL_TYPE_FORMULA
        if (cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
            return cell.getStringCellValue();
        }

// 4. 布尔CELL_TYPE_BOOLEAN
        if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
            return cell.getBooleanCellValue()+"";
        }

// 5.   错误 CELL_TYPE_ERROR
        return "";
    }
}



/**
 * @Description: 创建一个ExcelBean实现数据的封装
 * @author QB
 * @date 2017年10月17日 上午9:19:00
 */
class ExcelBean implements java.io.Serializable {
    /**
     * @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么)
     */
    private static final long serialVersionUID = 1L;
    private String headTextName;// 列头（标题）名
    private String propertyName;// 对应字段名
    private Integer cols;// 合并单元格数
    private XSSFCellStyle cellStyle;

    public ExcelBean() {

    }

    public ExcelBean(String headTextName, String propertyName) {
        this.headTextName = headTextName;
        this.propertyName = propertyName;
    }

    public ExcelBean(String headTextName, String propertyName, Integer cols) {
        super();
        this.headTextName = headTextName;
        this.propertyName = propertyName;
        this.cols = cols;
    }

    public String getHeadTextName() {
        return headTextName;
    }

    public void setHeadTextName(String headTextName) {
        this.headTextName = headTextName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public Integer getCols() {
        return cols;
    }

    public void setCols(Integer cols) {
        this.cols = cols;
    }

    public XSSFCellStyle getCellStyle() {
        return cellStyle;
    }

    public void setCellStyle(XSSFCellStyle cellStyle) {
        this.cellStyle = cellStyle;
    }

}

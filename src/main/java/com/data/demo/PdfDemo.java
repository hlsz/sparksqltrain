package com.data.demo;


import com.itextpdf.text.*;
import com.itextpdf.text.pdf.*;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class PdfDemo {

//    生成的pdf存放路径
    public static final String RESULT = "./demo.pdf";

    public static void main(String[] args) throws  DocumentException, IOException {

        Rectangle pagesize = new Rectangle(216f, 720f);
//      step 1   //创建文件
        // Document document = new Document(pagesize, 36f,72f, 108f,180f );
//      rotate 内容橫向显示
        Document document = new Document(PageSize.A5);
//      内存操作Pdf
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

//      step 2 //建立一个书写器
//      在内存中产生一个Pdf文件
        PdfWriter writer =  PdfWriter.getInstance(document, baos);
        writer.setPdfVersion(PdfWriter.VERSION_1_7);
//      PdfWriter.getInstance(document, new FileOutputStream(RESULT));

        // 对于需要装订成册的多个pdf，如果是左右装订（正反面打印）的话我们第一页的左边距要大一点，而第二页的右边距要和第一页的左边距一样，也就是保证页边距要对称
        document.setMargins(36,72,108,180);
        document.setMarginMirroring(true);

        // 用户密码
        String password= "123456";
        //拥有者密码
        String owenerPassword = "zjl";
        writer.setEncryption(password.getBytes(),
                owenerPassword.getBytes(),
                PdfWriter.ALLOW_PRINTING,
                PdfWriter.ENCRYPTION_AES_128);

        // 配置权限
        writer.setEncryption("".getBytes(),
                "".getBytes(),
                PdfWriter.ALLOW_PRINTING,
               PdfWriter.ENCRYPTION_AES_128 );


//      step 3 打开文件
        document.open();
        // 设置属性
        // 标题
        document.addTitle("this is a title");
        document.addAuthor("F_D");
        // 主题
        document.addSubject("this is subject");
        // 关键字
        document.addKeywords("keywords");
        // 创建时间
        document.addCreationDate();
        //应用程序
        document.addCreator("aa.com");

        // 添加表格
        PdfPTable table = new PdfPTable(3);
        table.setWidthPercentage(100);//宽度100%填充
        table.setSpacingBefore(10f); //前间距
        table.setSpacingAfter(10f); //后间距

        ArrayList<PdfPRow> listRow = table.getRows();
        //设置列宽
        float[] columnWidth = {1f, 2f, 3f};
        table.setWidths(columnWidth);

        //行1
        PdfPCell cells1[] = new PdfPCell[3];
        PdfPRow row1 = new PdfPRow(cells1);

        cells1[0] = new PdfPCell(new Paragraph("111"));
        cells1[0].setBorderColor(BaseColor.BLUE);//边框验证
        cells1[0].setPaddingLeft(20);//左填充20
        cells1[0].setHorizontalAlignment(Element.ALIGN_CENTER);
        cells1[0].setVerticalAlignment(Element.ALIGN_MIDDLE);

        cells1[1] = new PdfPCell(new Paragraph("222"));
        cells1[2] = new PdfPCell(new Paragraph("333"));

        //行2
        PdfPCell cells2[] = new PdfPCell[3];
        PdfPRow row2 = new PdfPRow(cells2);
        cells2[0] = new PdfPCell(new Paragraph("444"));

        listRow.add(row1);
        listRow.add(row2);
        document.add(table);




//  第一种方式  设置指定位置添加pdf的内容
        writer.setCompressionLevel(0);
        Phrase hello = new Phrase("Hello world");
        PdfContentByte canvas = writer.getDirectContentUnder();
        ColumnText.showTextAligned(canvas, Element.ALIGN_LEFT,
                hello, 36, 788, 0);

//  第二种方式 设置指定位置添加pdf的内容
//        PdfContentByte canvas = writer.getDirectContent();
//        writer.setCompressionLevel(0);
//        canvas.saveState();
//        canvas.beginText();
//        canvas.moveText(36, 788);
//        canvas.setFontAndSize(BaseFont.createFont(), 12);
//        canvas.showText("Hello Felix");
//        canvas.endText();
//        canvas.restoreState();


//      step 4 添加内容
        document.add(new Paragraph(
                "The left margin of this odd page is 36pt (0.5 inch); " +
                        "the right margin 72pt (1 inch); " +
                        "the top margin 108pt (1.5 inch); " +
                        "the bottom margin 180pt (2.5 inch)."
        ));
        Paragraph paragraph = new Paragraph();
        paragraph.setAlignment(Element.ALIGN_JUSTIFIED);
        for (int i = 0; i < 20; i++) {
            paragraph.add("Hello World! Hello People! " +
            "Hello Sky! Hello Sun! Hello Moon! Hello Stars!");
        }
        document.add(paragraph); // 添加内容
        document.add(new Paragraph("The right margin of this even page is 36pt (0.5 inch); " +
                "the left margin 72pt (1 inch)."));

        // 添加图片
        Image image = Image.getInstance("./demo.jpg");
        //设置图片位置的x轴和y轴
        image.setAbsolutePosition(100f, 550f);
        //设置图片的宽度和高度
        image.scaleAbsolute(200,200);
        //将图片1添加到pdf文件中
        document.add(image);

        Image image2 = Image.getInstance(new URL("http://www.baidu.com"));
        document.add(image2);

        // 添加有序列表
        List orderList = new List(List.ORDERED);
        orderList.add(new ListItem("Item one"));
        orderList.add(new ListItem("Item two"));
        orderList.add(new ListItem("Item three"));
        document.add(orderList);

        //设置样式、格式化输出 输出中文内容
        //中文字体，解决中文不能显示问题
        BaseFont bfChinese = BaseFont.createFont("STSong-Light","UniGB-UCS2-H",BaseFont.NOT_EMBEDDED);
        //蓝色字体
        Font blueFont = new Font(bfChinese);
        blueFont.setColor(BaseColor.BLUE);
        Paragraph paragraphBlue = new Paragraph("paragraphBlue", blueFont);
        //绿色字体
        Font greenFont = new Font(blueFont);
        greenFont.setColor(BaseColor.GREEN);
        //创建章节
        Paragraph chapterTitle = new Paragraph("段落标题MM",greenFont);
        Chapter chapter1 = new Chapter(chapterTitle, 1);
        chapter1.setNumberDepth(0);

        //部分标题
        Paragraph sectionTitle = new Paragraph("部分标题",greenFont);
        Section section1 = chapter1.addSection(sectionTitle);

        Paragraph sectionContent = new Paragraph("部分内空",greenFont);
        section1.add(sectionContent);
        //将章节添加到文章中
        document.add(chapter1);


        // 读取修改pdf文件
        // 读取pdf文件
        PdfReader pdfReader = new PdfReader("./demo.pdf");

        //修改器
        PdfStamper pdfStamper = new PdfStamper(pdfReader,
                new FileOutputStream("./demo2.pdf"));

        Image image3 = Image.getInstance("./pic.jpeg");
        image3.scaleAbsolute(50,50);
        image3.setAbsolutePosition(0, 700);

        for (int i = 0; i < pdfReader.getNumberOfPages(); i++) {
            PdfContentByte content = pdfStamper.getUnderContent(i);
            content.addImage(image);
        }


        //step 5 关闭文档
        document.close();
        // 关闭书写器
        writer.close();

        FileOutputStream fos = new FileOutputStream(RESULT);
        //      输出内存的pdf文件到本地
        fos.write(baos.toByteArray());

        fos.close();

        //      压缩多个pdf文本成一个文件
        ZipOutputStream zip =
        new ZipOutputStream(new FileOutputStream(RESULT));
        for (int i = 0; i <= 3; i++) {
            ZipEntry entry = new ZipEntry("hello_"+i + ".pdf");
            zip.putNextEntry(entry);
        }





    }
}

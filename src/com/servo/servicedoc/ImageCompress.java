/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.servicedoc;
import com.servo.mfdoc.MF_PSUPLOAD;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.Iterator;
import java.util.Map;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

/**
 *
 * @author VINITH
 */
public class ImageCompress {
    
    MF_PSUPLOAD mf_psupload = new MF_PSUPLOAD();

    public void picCompress(String SourceImge, String CompressImage, Connection con, Map dataMap) {
        try {
//        File imageFile = new File("E:\\Testing\\1-sig.jpg");
//        File compressedImageFile = new File("E:\\Testing\\1-sig_compressed.jpg");

            File imageFile = new File(SourceImge);
            File compressedImageFile = new File(CompressImage);

            OutputStream os;
            ImageWriter writer;
            ImageOutputStream ios;
            try (//
                    InputStream is = new FileInputStream(imageFile)) {
                os = new FileOutputStream(compressedImageFile);
                float quality = 0.2f;
                // create a BufferedImage as the result of decoding the supplied InputStream
                BufferedImage image = ImageIO.read(is);
                // get all image writers for JPG format
                Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpg");
                if (!writers.hasNext()) {
                    throw new IllegalStateException("No writers found");
                }
                writer = (ImageWriter) writers.next();
                ios = ImageIO.createImageOutputStream(os);
                writer.setOutput(ios);
                ImageWriteParam param = writer.getDefaultWriteParam();
                // compress to a given quality
                param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
                param.setCompressionQuality(quality);
                // appends a complete image stream containing a single image and
                //associated stream and image metadata and thumbnails to the output
                writer.write(null, new IIOImage(image, null, null), param);
                // close all streams
            }
            os.close();
            ios.close();
            writer.dispose();
        } catch (IOException | IllegalStateException e) {
            mf_psupload.updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "ERR", e.getMessage());
            System.out.println("Image COmpress exception:" + e.getMessage());

        }

    }

}

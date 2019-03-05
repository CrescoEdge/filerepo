package io.cresco.filerepo;

import java.io.File;

public class FileObject {

    public String fileName;
    public String MD5;
    String filePath;
    public String repo;

    public FileObject(String fileName, String MD5, String repo, String filePath) {
        this.fileName = fileName;
        this.MD5 = MD5;
        this.repo = repo;
        this.filePath = filePath;
    }

}

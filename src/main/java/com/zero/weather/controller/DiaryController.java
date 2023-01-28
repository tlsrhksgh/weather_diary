package com.zero.weather.controller;

import com.zero.weather.domain.Diary;
import com.zero.weather.service.DiaryService;
import io.swagger.annotations.ApiOperation;
import lombok.AllArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

@AllArgsConstructor
@RestController
public class DiaryController {
    private final DiaryService diaryService;

    @ApiOperation(value = "일기 텍스트와 날씨를 이용해서 DB에 일기 저장", notes = "일기 생성 api")
    @PostMapping("/create/diary")
    void createDiary(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
                     @RequestBody String text) {
        diaryService.createDiary(date, text);
    }

    @ApiOperation("선택한 날짜의 모든 일기 데이터를 가져옵니다.")
    @GetMapping("/read/diary")
    List<Diary> readDiary(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        return diaryService.readDiary(date);
    }

    @ApiOperation("선택한 기간중의 모든 일기 데이터를 가져옵니다.")
    @GetMapping("/read/diaries")
    List<Diary> readDiaries(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
                            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate) {
        return diaryService.readDiaries(startDate, endDate);
    }

    @PutMapping("/update/diary")
    void updateDiary(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
                     @RequestBody String text) {
        diaryService.updateDiary(date, text);
    }

    @DeleteMapping("/delete/diary")
    void deleteDiary(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        diaryService.deleteDiary(date);
    }

}

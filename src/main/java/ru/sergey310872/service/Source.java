package ru.sergey310872.service;

import ru.sergey310872.dto.SourceMessage;

interface Source {
    Iterable<SourceMessage> source();
}

# -*- coding=utf-8 -*-

def output(filename, data):
    with open(filename, 'w') as f:
        f.write((
            '95-й перцентиль времени работы: {}\n\n' 
            'Идентификаторы запросов с самой долгой фазой отправки результатов пользователю:\n{}\n\n' \
            'Запросов с неполным набором ответивших ГР: {}\n\n')
            .format(data['p95'], ' '.join(data['top10']), data['fails'])
        )

        f.write('Обращения и ошибки по бекендам:\n')
        for gr_key in set(data['ok'].viewkeys()) | set(data['err'].viewkeys()):
            f.write('ГР {}:\n'.format(gr_key))
            
            ok = data['ok'][gr_key]
            err = data['err'][gr_key]
            
            for url_key in set(ok.viewkeys()) | set(err.viewkeys()):
                f.write('\t{}\n'.format(url_key))

                total = ok[url_key] + sum(err[url_key].values())
                f.write('\t\tОбращения: {}\n'.format(total))
                
                errors = err[url_key]
                if errors:
                    f.write('\t\tОшибки:\n')
                    for k, v in errors.iteritems():
                        f.write('\t\t\t{}: {}\n'.format(k, v))

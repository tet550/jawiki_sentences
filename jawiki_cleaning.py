import mwparserfromhell
import bz2
import re
import os
from lxml import etree
import mojimoji
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def process_dump(dump_file):
    """
    ダンプファイルから記事を読み込む
    :param dump_file: ダンプファイル
    :return: 記事毎に「タイトル、トピック、テキスト」のリスト
    """
    with bz2.open(dump_file, "rb") as f:
        context = etree.iterparse(f, events=("end",), tag="{http://www.mediawiki.org/xml/export-0.10/}page")
        for event, elem in context:
            title = elem.findtext("{http://www.mediawiki.org/xml/export-0.10/}title")
            text = elem.findtext(".//{http://www.mediawiki.org/xml/export-0.10/}text")

            # 不要なタイトルを除外
            if title.startswith('Wikipedia:') == False and \
                title.startswith('Category:') == False and \
                title.startswith('Template:') == False and \
                title.startswith('Help:') == False and \
                title.startswith('Portal:') == False and \
                title.startswith('プロジェクト:') == False and \
                title.startswith('モジュール:') == False and \
                title.startswith('特別:') == False and \
                "一覧" not in title and \
                text.startswith("#REDIRECT") == False and \
                text.startswith("#redirect") == False and \
                text.startswith("#転送") == False and \
                "{{aimai}}" not in text and \
                "{{Aimai}}" not in text:
                yield list(process_article(title, text))

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0] 

sections_to_remove = ["関連項目", "外部リンク", "参考文献", "Further readings", "注釈", "脚注", "出典", "個人成績"]
def remove_sections(text):
    """
    記事から不要なセクションを除去する
    :param text: 記事
    :return: 不要なセクションを除去した記事
    """
    parsed_text = mwparserfromhell.parse(text)
    for section in parsed_text.get_sections():
        headings = section.filter_headings()
        if not headings:
            continue
        section_title = headings[0].title.strip()
        if section_title in sections_to_remove or \
            str.endswith(section_title, '一覧') or \
            str.endswith(section_title, '文献') or \
            str.endswith(section_title, '分類') or \
            str.endswith(section_title, '分野') or \
            str.endswith(section_title, 'リスト') or \
            str.endswith(section_title, '作品') :
            parsed_text.remove(section)
    return str(parsed_text)

def remove_wiki_elements(text):
    """
    記事から不要なWiki要素を除去する
    :param text: 記事
    :return: 不要なWiki要素を除去した記事
    """
    # <!--ANY-->
    text = re.sub(r'\<\!--.*?--\>', '', text, flags=re.DOTALL)
    # <ref \>
    text = re.sub(r'\<ref[^\>]*?\/\>', '', text, flags=re.IGNORECASE)
    # <ref>ANY</ref>
    text = re.sub(r'\<ref.*?\<\/ref\>', '', text, flags=re.DOTALL|re.IGNORECASE)
    # <gallery>ANY</gallery>
    text = re.sub(r'\<gallery.*?\<\/gallery\>', '', text, flags=re.DOTALL|re.IGNORECASE)
    # <timeline>ANY</timeline>
    text = re.sub(r'\<timeline.*?\<\/timeline\>', '', text, flags=re.DOTALL|re.IGNORECASE)
    # <table>ANY</table>
    text = re.sub(r'\<table.*?\<\/table\>', '', text, flags=re.DOTALL|re.IGNORECASE)
    # <imagemap>ANY</imagemap>
    text = re.sub(r'\<imagemap.*?\<\/imagemap\>', '', text, flags=re.DOTALL|re.IGNORECASE)
    # <score>ANY</score>
    text = re.sub(r'\<score.*?\<\/score\>', '', text, flags=re.DOTALL|re.IGNORECASE)
    # <syntaxhighlight>ANY</syntaxhighlight>
    text = re.sub(r'\<syntaxhighlight.*?\<\/syntaxhighlight\>', '', text, flags=re.DOTALL|re.IGNORECASE)
    # <br \>
    text = re.sub(r'\<br *\/?\>', ' ', text)
    # <div>
    text = re.sub(r'\<\/?(div|span|blockquote|nowiki|var|small|center|ins|ol|li|em|u|s)[^\>]*?\>', '', text, flags=re.IGNORECASE)
    # <ref>ANY</ref> 削除
    text = re.sub(r'\<ref.*?\<\/ref\>', '', text, flags=re.MULTILINE)
    # <code stye=111> -> <code>
    text = re.sub(r'\<(\w+) [^\>\/]*\>', r'<\1>', text)
    # {{R|ANY}} 削除
    text = re.sub(r'｛(R|coord|Flatlist|formatnum|Gallery|sfn|harv|要|出典|\#|\-)[^｛｝]*?｝', '', text, flags=re.DOTALL|re.IGNORECASE)
    # [[ファイル|ANY]] 削除
    text = re.sub(r'［(ファイル|File|画像|:ファイル|Media)[^［］]*?］', '', text, flags=re.DOTALL|re.IGNORECASE)
    # __TOC__ 削除
    text = re.sub(r'__TOC__', '', text, flags=re.DOTALL)
    # [https://..] 削除
    text = re.sub(r'\[http.*?\]', '', text, flags=re.IGNORECASE)
    # #FFAA1122　削除
    text = re.sub(r'#[0-9A-Fa-f]{6}', '', text)
    # *など
    text = re.sub(r'^[\*\#\;\:]+ *', '', text, flags=re.MULTILINE)
    # ==ANY== 削除
    text = re.sub(r'^\=[\=]*[ ]*(.*?)[ ]*[\=]*\=$', r'# \1', text, flags=re.MULTILINE)
    # {{ANY}} 削除
    text = re.sub(r'^｛[^｛｝].*｝$', r'', text, flags=re.MULTILINE)
    # [[ANY]] 削除
    text = re.sub(r'^［[^［］].*］$', r'', text, flags=re.MULTILINE)
    return text

def remove_wiki_tag(text):
    """
    記事から不要なWikiタグを除去する
    :param text: 記事
    :return: 不要なWikiタグを除去した記事
    """
    count = 1
    while count > 0:
        text, count_a = re.subn(r'''
        ｛
        (?:lang\|)?
        (?:[^｛｝［］\|]*?\|)?
        (?:[^｛｝［］\|\=]*?\=)?
        (?:\:\w+\:)?
        ([^｛｝［］\|]*?)?
        (?:\|[^｛｝［］]*?)?
        ｝
        ''', r'\1', text, flags=re.DOTALL|re.VERBOSE|re.IGNORECASE)
        # {{R|ANY}} 削除
        text = re.sub(r'｛(R|coord|Flatlist|formatnum|Gallery|sfn|harv|要|出典|\#|\-)[^｛｝]*?｝', '', text, flags=re.DOTALL|re.IGNORECASE)
        text, count_b = re.subn(r'''
        ［
        (?:[^｛｝［］\|]*?\|)?
        (?:\:\w+\:)?
        ([^｛｝［］\|]*?)?
        (?:\|[^｛｝［］]*?)?
        ］
        ''', r'\1', text, flags=re.DOTALL|re.VERBOSE)
        # [[ファイル|ANY]] 削除
        text = re.sub(r'［(ファイル|File|画像|:ファイル|Media)[^［］]*?］', '', text, flags=re.DOTALL|re.IGNORECASE)
        text, count_c = re.subn(r'''
        〔
        (?:[^〔〕]*?)
        〕
        ''', '', text, flags=re.VERBOSE|re.DOTALL)
        count = count_a + count_b + count_c       
    return text

def convert_fullwidth_to_halfwidth(text):
    """
    全角文字を半角文字に変換する
    :param text: 記事
    :return: 全角文字を半角文字に変換した記事
    """
    return mojimoji.zen_to_han(text, kana=False, ascii=True, digit=True)

def remove_parentheses_and_special_chars(text):
    """
    括弧と特殊文字を除去する
    :param text: 記事
    :return: 括弧と特殊文字を除去した記事
    """
    text = re.sub(r'[\t]+', ' ', text)             # タブ文字の削除
    text = re.sub(r'[\u3000\xa0]+', ' ', text)       # 全角・半角スペースの削除
    text = re.sub(r'^\s*\w+\s*$\n?', '', text, flags=re.MULTILINE) # ゴミ制御文削除
    text = re.sub(r'^[ ]*$\n', '', text, flags=re.MULTILINE) # 空行の削除
    return text

def process_article(title, text):
    """
    記事を整形する
    :param title: 記事タイトル
    :param text: 記事
    :return: タイトル、トピック、テキスト
    """
    # 前処理
    text = re.sub(r'｛', ' {', text)
    text = re.sub(r'｝', '} ', text)
    text = re.sub(r'［', ' [', text)
    text = re.sub(r'］', '] ', text)
    text = re.sub(r'〔', ' [', text)
    text = re.sub(r'〕', '] ', text)
    text = re.sub(r'\{\{', '｛', text)
    text = re.sub(r'\}\}', '｝', text)
    text = re.sub(r'\[\[', '［', text)
    text = re.sub(r'\]\]', '］', text)
    text = re.sub(r'\{\|', '〔', text)
    text = re.sub(r'\|\}', '〕', text)

    text = remove_sections(text)
    text = remove_wiki_elements(text)
    text = remove_wiki_tag(text)
    text = convert_fullwidth_to_halfwidth(text)
    text = remove_parentheses_and_special_chars(text)

    lines = text.split('\n')
    topic_title = ''

    for line in lines:
        if line.startswith('# '):
            topic_title = line[2:]
        elif line:
            yield (title, topic_title, line)

def write_output(file_path, output_dir, max_file_size):
    """
    ダンプファイルを読み込み、Parquetファイルに書き出す
    :param file_path: ダンプファイルのパス
    :param output_dir: Parquetファイルの出力先ディレクトリ
    :param max_file_size: Parquetファイルの最大サイズ(圧縮前のため、実際のファイルサイズはこれより小さくなる)
    """    
    os.makedirs(output_dir, exist_ok=True)

    schema = pa.schema([('article_title', pa.string()), 
                        ('topic_title', pa.string()), 
                        ('text', pa.string())])

    file_counter = 1
    writer = pq.ParquetWriter(f'{output_dir}{file_counter}.parquet', schema)
    
    current_size = 0
    article_counter = 1
    for article_list in process_dump(file_path):

        if article_counter % 10000 == 0:
            print(f'{article_counter} articles processed.')
        article_counter += 1
        
        dataframe = pd.DataFrame(article_list, columns=['article_title', 'topic_title', 'text'])
        estimated_size = dataframe.memory_usage(index=True, deep=True).sum()

        if current_size + estimated_size > max_file_size:
            writer.close()
            file_counter += 1
            writer = pq.ParquetWriter(f'{output_dir}{file_counter}.parquet', schema)
            current_size = 0

        batch = pa.RecordBatch.from_pandas(dataframe, schema=schema)
        writer.write_table(pa.Table.from_batches([batch]))
        current_size += estimated_size
    writer.close()


if __name__ == '__main__':

    file_path = "data_raw/jawiki-latest-pages-articles.xml.bz2"
    output_path = "data/"
    max_file_size = 200 * 1024 * 1024 
    max_file_size = max_file_size * 5 / 2 #圧縮率を考慮
    
    write_output(file_path, output_path, max_file_size)

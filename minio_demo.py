from minio import Minio
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from datetime import timedelta
from minio.error import S3Error
import ffmpeg
import subprocess
import os

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:5173"}})

ACCESS_KEY = 'admin'
SECRET_KEY = '12345678'

# 连接minio存储服务
minio_client = Minio('192.168.2.13:9000',
                    access_key = ACCESS_KEY,
                    secret_key = SECRET_KEY,
                    secure = False
                    )

@app.route("/minio/upload", methods = ['post'])
def minio_upload():
    params = request.form if request.form else request.json
    # 接收文件
    file_name = params.get('file')
    bucket_name = params.get('bucket')
    # file_name = file.filename
    # # 重新定向指针到文件开头
    # file.stream.seek(0)
    # # 读取文件大小
    # file_size = len(file.stream.read())
    # # 重新定向指针到文件开头
    # file.stream.seek(0)
    # # file_stream = BytesIO(file.read())
    # print('文件大小: ', file_size)
    # print('文件流: ',file.stream)
    print('文件名: ', file_name)
    print('桶名: ', bucket_name)
    # 桶名称
    # bucket_name = "dem"
    # 验证要存放的桶是否存在
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
        print('新建桶 {} 成功！'.format(bucket_name))
    else:
        print("桶 '{}' 存在".format(bucket_name))
    # 上传的链接 有效期两个小时
    url = minio_client.presigned_put_object(bucket_name, file_name, expires=timedelta(hours=2))
    print(url)

    # # minio_client.put_object(bucket_name, '/'+ file_name, file, file_size)
    # print('success')
    # file.close
    response = {
        # "message": f"文件存到了{bucket_name + '/' + file_name}",
        "url": url
        # 'url':'http://127.0.0.1:8081/minio/upload_test'
    }
    # return render_template('upload.html', url = url)
    return response

@app.route('/web')
def web():
    return render_template('upload.html')

@app.route('/minio/download', methods=['post'])
def download():
    
    params = request.form if request.form else request.json
    # 从请求中获取文件名和存储桶名
    file_name = params.get('file')
    # 桶名称
    bucket_name = params.get('bucket')
    print('file_name:', file_name)
    print('bucket:', bucket_name)
    # 验证要存放的桶是否存在
    bucket_found = minio_client.bucket_exists(bucket_name)
    if not bucket_found:
        print('桶 {} 不存在'.format(bucket_name))
        return jsonify({'message': '桶不存在'})
    else:
        print("桶 '{}' 存在".format(bucket_name))
        try:
            file_found = minio_client.stat_object(bucket_name, file_name)
            print('文件 {} 存在'.format(file_name))
            # 生成预签名URL
            presigned_url = minio_client.presigned_get_object(bucket_name, file_name, expires=timedelta(hours=2))
            print('presigned_url:', presigned_url)
            return jsonify({'presigned_url': presigned_url})
        except S3Error as err:
            if  err.code=='NoSuchKey':
                print('文件 {} 不存在'.format(file_name))
                return jsonify({'message': '桶存在，文件不存在'})
            else:
                return jsonify({'message': f'其他错误-->{err}'})


@app.route('/minio/compress', methods=['post'])
def compress():
    data = request.get_json()
    object_context = data["Records"][0]['s3']
    bucket_name  = object_context['bucket']['name']
    file_name = object_context['object']['key']
    input_file = file_name
    output_file = f'compress-{input_file}'

    # 取文件
    file = minio_client.get_object(bucket_name, file_name)
    print(file)
    if not os.path.exists('./input'):
        os.makedirs('./input')
    with open(f'./input/{input_file}', "wb") as file_in:
        for data in file_in:
            file_in.write(data)
    # ffmpeg指令
    command = f'ffmpeg -i ./input/{input_file} -c:v libx264 ./input/{output_file}'
    # 启动ffmpeg进程
    if os.path.exists(f'./input/{input_file}'):
        ffmpeg_process = subprocess.Popen(command)
        print('Start!!!')
        # 等待进程完成
        ffmpeg_process.wait()
        # 检查进程的返回码
        if ffmpeg_process.returncode == 0:
            print('OK!!!')
        else:
            print('Error!!!')

        # 关闭FFmpeg进程
        ffmpeg_process.kill()
    # 删除本地文件
    # os.remove(f'./input/{input_file}')
    print(data)
    print(bucket_name)
    print(file_name)
    # 上传压缩文件
    # 验证要存放的桶是否存在
    compress_bucket = 'compress'
    found = minio_client.bucket_exists(compress_bucket)
    if not found:
        minio_client.make_bucket(compress_bucket)
        print('新建桶 {} 成功！'.format(compress_bucket))
    else:
        print("桶 '{}' 存在".format(compress_bucket))
    # 上传
    minio_client.fput_object(compress_bucket, output_file, f'./input/{output_file}')
    # 
    # 删除本地文件
    # os.remove(f'./input/{output_file}')
    print('文件上传到：', f'{compress_bucket}/{output_file}')
    print('压缩处理完成')
    return ('压缩处理完成') 

@app.route("/minio/get_url", methods = ['post'])
def get_url():
    params = request.form if request.form else request.json
    file_name = params.get('fileName')
    bucket_name = 'vuetest'
    print(file_name)
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
        print('新建桶 {} 成功！'.format(bucket_name))
    else:
        print("桶 '{}' 存在".format(bucket_name))
    # 上传的链接 有效期两个小时
    url = minio_client.presigned_put_object(bucket_name, file_name, expires=timedelta(hours=2))
    print(url)
    response = {
        "url": url
    }
    return response



@app.route('/minio/upload_test', methods=['put'])
def upload_test():
    # params = request.form if request.form else request.json
    file = request.files['file']
    # bucket = params.get('bucket')
    name = file.filename
    print(file)
    print(name)
    print(type(file))
    # print(name)
    if not os.path.exists('./input'):
        os.makedirs('./input')
    with open(f'./input/{name}', "wb") as file_in:
        file_in.write(file.read())
    return 'ok'

@app.route('/minio/upload_success', methods=['post'])
def upload_success():
    message = request.get_data()
    print(message)
    response = {
        'message': '可以下载原视频啦！',
        'url': 'http://127.0.0.1:8081/minio/download_url',
        'bucketName': 'vuetest',
        'objectName': 'mov1.mp4'
    }
    return response

@app.route('/minio/download_url', methods=['post'])
def download_url():
    # params = request.form if request.form else request.json
    # file_name = params.get('file')
    message = request.get_data()
    print(message)

    response = {
        "compressUrl": 'http://127.0.0.1:9000/vuetest/mov1.mp4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=KB937TUUMTQOL1X91G4U%2F20231205%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20231205T085051Z&X-Amz-Expires=604800&X-Amz-Security-Token=eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJLQjkzN1RVVU1UUU9MMVg5MUc0VSIsImV4cCI6MTcwMTc4ODAxNSwicGFyZW50IjoiYWRtaW4ifQ.DRbrriI_IzfgSYrCtcSJ1dp6Eu3S0WPI2AdGXwklsdaFP5fgLjjcEue2ZIJlKkXq1nwhUUQp5QD3zYDR93cqZQ&X-Amz-SignedHeaders=host&versionId=null&X-Amz-Signature=97301dc639b86c122b9719bc58e9a4ab23157a9020266331262e14ac58daecc6',
        "infoUrl" : '',
        "frameUrl" : '',
    }
    return response

if __name__ == '__main__':
    # app.run(debug=True)
    app.jinja_env.auto_reload = True
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run('0.0.0.0',port=8081, debug=True)
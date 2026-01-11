FROM google/cloud-sdk:slim

RUN apt-get update && apt-get install -y python3-pip python3-venv

WORKDIR /dbt

RUN python3 -m venv /opt/venv
ENV PATH="opt/venv/bin:$PATH"

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /root/.dbt
COPY ./profiles.yml /root/.dbt/profiles.yml

ENTRYPOINT ["dbt"]
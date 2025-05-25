#!/bin/bash

# Configs
NUM_CLIENTS=10       
EVENTS_PER_CLIENT=5  
CLIENT_SCRIPT_DIR="../grpc"
CLIENT_SCRIPT_NAME="client.py" 
TMUX_SESSION_NAME="grpc_clients" 


echo "Iniciando $NUM_CLIENTS clientes"
echo "O servidor gRPC (./exes/server.exe) deve estar ativo para essa execução!"

# Verifica se outra sessão tmux já existe
tmux has-session -t "$TMUX_SESSION_NAME" 2>/dev/null

if [ $? != 0 ]; then
    echo "Criando nova sessão tmux: $TMUX_SESSION_NAME"
    # Primeiro cliente
    tmux new-session -s "$TMUX_SESSION_NAME" -d "cd '$CLIENT_SCRIPT_DIR' && python3 '$CLIENT_SCRIPT_NAME' 1 '$EVENTS_PER_CLIENT'; echo 'Cliente 1 finalizado.'; exec bash"
    sleep 0.3 # Pequena pausa

    # Outros clientes
    for i in $(seq 2 $NUM_CLIENTS); do
        echo "Criando janela tmux para Cliente GRPC #$i..."
        tmux new-window -t "$TMUX_SESSION_NAME:$i" -n "Client-$i" "cd '$CLIENT_SCRIPT_DIR' && python3 '$CLIENT_SCRIPT_NAME' 1 '$EVENTS_PER_CLIENT'; echo 'Cliente $i finalizado.'; exec bash"
        sleep 0.3 # Pequena pausa
    done

    echo "Clientes Iniciados '$TMUX_SESSION_NAME'."
    echo "Para acessar os clientes, digite: tmux attach -t $TMUX_SESSION_NAME"
    echo "Para alternar entre os clientes, digite: Ctrl+b, depois (n) para próxima ou (p) para anterior."
    echo "Para fechar uma janela: Ctrl+d"
else
    echo "Sessão já existe"
    echo "Para anexar a sessão existente: tmux attach -t $TMUX_SESSION_NAME"
fi